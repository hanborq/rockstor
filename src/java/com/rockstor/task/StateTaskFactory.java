/**
 * Copyright 2012 Hanborq Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rockstor.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;

import com.rockstor.exception.ProcessException;
import com.rockstor.util.Base64;
import com.rockstor.util.DateUtil;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.TAG;
import com.rockstor.webifc.data.AccessControlList;
import com.rockstor.webifc.data.CompletePartList;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.Req;
import com.rockstor.webifc.req.ReqAbortMultiPart;
import com.rockstor.webifc.req.ReqCompleteMultiPart;
import com.rockstor.webifc.req.ReqCopyObject;
import com.rockstor.webifc.req.ReqDelBucket;
import com.rockstor.webifc.req.ReqDelObject;
import com.rockstor.webifc.req.ReqDeleteMeta;
import com.rockstor.webifc.req.ReqGetBucket;
import com.rockstor.webifc.req.ReqGetBucketAcl;
import com.rockstor.webifc.req.ReqGetMeta;
import com.rockstor.webifc.req.ReqGetObject;
import com.rockstor.webifc.req.ReqGetObjectAcl;
import com.rockstor.webifc.req.ReqGetService;
import com.rockstor.webifc.req.ReqHeadObject;
import com.rockstor.webifc.req.ReqInitMultiPart;
import com.rockstor.webifc.req.ReqListMultiParts;
import com.rockstor.webifc.req.ReqListParts;
import com.rockstor.webifc.req.ReqMoveObject;
import com.rockstor.webifc.req.ReqPutBucket;
import com.rockstor.webifc.req.ReqPutBucketAcl;
import com.rockstor.webifc.req.ReqPutObject;
import com.rockstor.webifc.req.ReqPutObjectAcl;
import com.rockstor.webifc.req.ReqPutObjectMeta;
import com.rockstor.webifc.req.ReqUploadPart;

public class StateTaskFactory {
    private static Logger LOG = Logger.getLogger(StateTaskFactory.class);
    private static StateTaskFactory instance = null;

    private StateTaskFactory() {
    }

    public static StateTaskFactory getInstance() {
        if (instance == null) {
            instance = new StateTaskFactory();
        }
        return instance;
    }

    public StateTask<? extends Req> createTask(HttpServletRequest request,
            HttpServletResponse response) throws ProcessException {
        // parse Method
        String method = request.getMethod();
        // parse url
        String url = null;
        try {
            url = URLDecoder.decode(request.getRequestURI(), "UTF-8");
        } catch (UnsupportedEncodingException e2) {
            LOG.error("Url format Error : " + request.getRequestURI());
            throw new ProcessException(StatusCode.ERR400_InvalidURI);
        }
        // LOG.info("REQ URI : "+url);
        // parse /bucket/object
        String bucket = null;
        String object = null;

        String bucketobject = request.getPathInfo().substring(1).trim();
        // LOG.info("request path info: "+request.getPathInfo()+", bucketObject:  "+bucketobject);
        int splitIndex = bucketobject.indexOf("/");
        if (bucketobject.isEmpty()) {
            bucket = null;
            object = null;
        } else if (splitIndex < 0) {
            bucket = bucketobject;
            object = null;
        } else {
            bucket = bucketobject.substring(0, splitIndex);
            try {
                String tmp = bucketobject.substring(splitIndex);
                // object = new String(tmp.getBytes("iso-8859-1"),"UTF-8");
                object = tmp;
                // LOG.info("REQ object : "+object);
            } catch (Exception e) {
                throw new ProcessException(StatusCode.ERR400_InvalidURI);
            }
        }
        // parse Date
        long date = 0;
        String dateString = "";
        try {
            dateString = request.getHeader(TAG.DATE);
            date = DateUtil.str2Long(dateString);
        } catch (Exception e) {
        }

        if (Math.abs(date - System.currentTimeMillis()) > 86400000) {
            throw new ProcessException(StatusCode.ERR403_RequestTimeTooSkewed);
        }

        // parse ContentType
        String contentType = request.getContentType();
        // parse Authorization
        String authorization = request.getHeader(TAG.AUTHORIZATION);

        // construct Workers
        if (method.equals("GET")) {
            if (bucket == null && object == null) {
                ReqGetService req = new ReqGetService();
                req.setBasicAttr(method, url, bucket, object, null, date,
                        dateString, contentType, 0, authorization);
                return new GetServiceTask(req, response, request);
            } else if (bucket != null && object == null) {
                if (request.getParameter(TAG.ACL) != null) {
                    ReqGetBucketAcl req = new ReqGetBucketAcl();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    return new GetBucketAclTask(req, response, request);
                } else {
                    String marker = request.getParameter(TAG.MARKER);
                    String maxKeysString = request.getParameter(TAG.MAX_KEYS);
                    String prefix = null;
                    try {
                        prefix = request.getParameter(TAG.PREFIX);
                        if (prefix != null)
                            // prefix = new
                            // String(request.getParameter(TAG.PREFIX).getBytes("iso-8859-1"),"UTF-8");
                            prefix = URLDecoder.decode(prefix, "UTF-8");
                    } catch (Exception e1) {
                        throw new ProcessException(
                                StatusCode.ERR400_InvalidArgument);
                    }
                    String delimiter = request.getParameter(TAG.DELIMITER);
                    int maxKeys = 0;
                    if (maxKeysString != null) {
                        try {
                            maxKeys = Integer.parseInt(maxKeysString);
                        } catch (Exception e) {
                        }
                    }
                    ReqGetBucket req = new ReqGetBucket();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    req.setMarker(marker);
                    req.setMaxKeys(maxKeys);
                    req.setPrefix(prefix);
                    req.setDelimiter(delimiter);
                    return new GetBucketTask(req, response, request);
                }
            } else if (bucket != null && object != null) {
                // check if delete meta
                String qStr = request.getQueryString();
                if (qStr != null && qStr.startsWith(TAG.META)) {
                    String[] metas = request.getParameterValues(TAG.META);
                    Set<String> validKeys = new HashSet<String>();
                    if (metas != null) {
                        LOG.info("Get Object Meta, key num: " + metas.length);
                        for (String k : metas) {
                            if (k.isEmpty()) {
                                continue;
                            }
                            validKeys.add(k.trim());
                        }
                        ReqGetMeta req = new ReqGetMeta();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, 0, authorization);
                        req.setMetas(validKeys.toArray(new String[0]));

                        return new GetMetaTask(req, response, request);
                    }
                }

                String uploadId = request.getParameter(TAG.UPLOAD_ID);

                if (uploadId != null) { // list parts
                    if (uploadId.length() != UploadIdGen.UPLOAD_ID_STR_LEN) {
                        throw new ProcessException(
                                StatusCode.ERR404_NoSuchUpload);
                    }

                    ReqListParts req = new ReqListParts();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    req.setUploadId(uploadId);
                    return new ListPartsTask(req, response, request);
                }

                // list multipart objects
                String uploads = request.getParameter(TAG.UPLOADS);

                if (uploads != null) {
                    String keyMarker = request.getParameter(TAG.KEY_MARKER);
                    String uploadIdMarker = request
                            .getParameter(TAG.UPLOAD_ID_MARKER);

                    String maxKeysString = request
                            .getParameter(TAG.MAX_UPLOADS);

                    String prefix = request.getParameter(TAG.PREFIX);
                    String delimiter = request.getParameter(TAG.DELIMITER);

                    int maxKeys = 0;
                    if (maxKeysString != null) {
                        try {
                            maxKeys = Integer.parseInt(maxKeysString);
                        } catch (Exception e) {
                        }
                    }
                    ReqListMultiParts req = new ReqListMultiParts();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    req.setKeyMarker(keyMarker);
                    req.setUploadIdMarker(uploadIdMarker);
                    req.setMaxUploads(maxKeys);
                    req.setPrefix(prefix);
                    req.setDelimiter(delimiter);
                    return new ListMultiPartTask(req, response, request);
                }

                if (request.getParameter(TAG.ACL) != null) {
                    ReqGetObjectAcl req = new ReqGetObjectAcl();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    return new GetObjectAclTask(req, response, request);
                } else {

                    ReqGetObject req = new ReqGetObject();

                    if (authorization == null) {
                        String _authorization = request
                                .getParameter(TAG.AUTHORIZATION);
                        String _dateString = request.getParameter(TAG.DATE);
                        if (_authorization != null) {
                            try {
                                byte[] b = Base64.decode(_authorization
                                        .getBytes());
                                authorization = new String(b);
                                LOG.info("auth from url : auth : "
                                        + authorization);
                                b = Base64.decode(_dateString.getBytes());
                                dateString = new String(b);
                                date = DateUtil.str2Long(dateString);
                                LOG.info("auth from url : date : " + dateString);
                            } catch (Exception e) {
                                LOG.warn("get authorization from url, but parse error : "
                                        + _authorization);
                                authorization = null;
                            }
                            req.setFromWeb(true);
                        }
                    }

                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    req.setIfMatch(request.getParameter(TAG.IF_MATCH));
                    req.setIfModifiedSince(request
                            .getParameter(TAG.IF_MODIFIED_SINCE));
                    req.setIfNoneMatch(request.getParameter(TAG.IF_NONE_MATCH));
                    req.setIfUnmodifiedSince(request
                            .getParameter(TAG.IF_UNMODIFIED_SINCE));
                    return new GetObjectTask(req, response, request);
                }
            }
        } else if (method.equals("PUT")) {

            // parse ContentLength
            String contentLengthString = request.getHeader(TAG.CONTENT_LENGTH);
            if (contentLengthString == null) {
                throw new ProcessException(
                        StatusCode.ERR411_MissingContentLength);
            }
            long contentLength = 0;
            try {
                contentLength = Long.parseLong(contentLengthString);
            } catch (Exception e) {
            }

            if (bucket == null && object == null) {
                LOG.error("PUT request should be applied to a bucket or an object.");
                throw new ProcessException(StatusCode.ERR400_InvalidURI);
            } else if (bucket != null && object == null) {
                if (request.getParameter(TAG.ACL) != null) {
                    InputStream input = null;
                    try {
                        input = request.getInputStream();
                        AccessControlList acl = (AccessControlList) XMLData
                                .deserialize(AccessControlList.class, input);
                        ReqPutBucketAcl req = new ReqPutBucketAcl();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, contentLength,
                                authorization);
                        req.setAcl(acl);
                        return new PutBucketAclTask(req, response, request);
                    } catch (IOException e) {
                        LOG.error("read ServletInputStream catch IOException : "
                                + ExceptionLogger.getStack(e));
                        throw new ProcessException(
                                StatusCode.ERR400_MalformedACLError);
                    } catch (JAXBException e) {
                        LOG.error("parse xml data catch JAXBException : "
                                + ExceptionLogger.getStack(e));
                        throw new ProcessException(
                                StatusCode.ERR400_MalformedXML);
                    } finally {
                        if (input != null)
                            try {
                                input.close();
                            } catch (IOException e) {
                                LOG.error("close ServletInputStream error : "
                                        + ExceptionLogger.getStack(e));
                            }
                    }
                } else {
                    ReqPutBucket req = new ReqPutBucket();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, contentLength,
                            authorization);
                    req.setRockstorAcl(request.getHeader(TAG.ROCKSTOR_ACL));
                    return new PutBucketTask(req, response, request);
                }
            } else if (bucket != null && object != null) {
                String partNumberStr = request.getParameter(TAG.PART_NUMBER);
                String uploadId = request.getParameter(TAG.UPLOAD_ID);

                if (partNumberStr != null || uploadId != null) { // upload part
                    if (partNumberStr == null || uploadId == null) {
                        LOG.error("partNumber or uploadId is not specifiled while uploading part.");
                        throw new ProcessException(StatusCode.ERR400_InvalidURI);
                    }

                    short partNumber = Short.parseShort(partNumberStr);
                    if (partNumber <= 0) {
                        LOG.error("partNumber should greater than zero while uploading part.");
                        throw new ProcessException(StatusCode.ERR400_InvalidURI);
                    }

                    if (uploadId.length() != UploadIdGen.UPLOAD_ID_STR_LEN) {
                        throw new ProcessException(
                                StatusCode.ERR404_NoSuchUpload);
                    }

                    ReqUploadPart req = new ReqUploadPart();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, contentLength,
                            authorization);
                    try {
                        req.setInput(request.getInputStream());
                        req.setPartNumber(partNumber);
                        req.setUploadId(uploadId);
                    } catch (IOException e) {
                        LOG.error("open ServletInputStream error : "
                                + ExceptionLogger.getStack(e));
                        throw new ProcessException(
                                StatusCode.ERR400_IncompleteBody);
                    }
                    return new UploadPartTask(req, response, request);

                } else if (request.getParameter(TAG.ACL) != null) {
                    InputStream input = null;
                    try {
                        input = request.getInputStream();
                        AccessControlList acl = (AccessControlList) XMLData
                                .deserialize(AccessControlList.class, input);
                        ReqPutObjectAcl req = new ReqPutObjectAcl();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, contentLength,
                                authorization);
                        req.setAcl(acl);
                        return new PutObjectAclTask(req, response, request);
                    } catch (IOException e) {
                        LOG.error("read ServletInputStream catch IOException : "
                                + ExceptionLogger.getStack(e));
                        throw new ProcessException(
                                StatusCode.ERR400_MalformedACLError);
                    } catch (JAXBException e) {
                        e.printStackTrace();
                        LOG.error("parse xml data catch JAXBException : " + e);
                        throw new ProcessException(
                                StatusCode.ERR400_MalformedXML);
                    } finally {
                        if (input != null)
                            try {
                                input.close();
                            } catch (IOException e) {
                                LOG.error("close ServletInputStream error : "
                                        + ExceptionLogger.getStack(e));
                            }
                    }
                } else if (request.getParameter(TAG.META) != null) {
                    InputStream input = null;
                    try {

                        ReqPutObjectMeta req = new ReqPutObjectMeta();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, contentLength,
                                authorization);

                        // get metas from header
                        HashMap<String, String> metas = new HashMap<String, String>();
                        @SuppressWarnings("unchecked")
                        Enumeration<String> names = request.getHeaderNames();
                        while (names.hasMoreElements()) {
                            String headName = names.nextElement();
                            if (headName.indexOf(TAG.ROCKSTOR_MATA_) == 0)
                                metas.put(
                                        headName.substring(TAG.ROCKSTOR_META_PREFIX_LENGTH),
                                        request.getHeader(headName));
                        }

                        req.setMetas(metas);
                        return new PutObjectMetaTask(req, response, request);
                    } finally {
                        if (input != null)
                            try {
                                input.close();
                            } catch (IOException e) {
                                LOG.error("close ServletInputStream error : "
                                        + ExceptionLogger.getStack(e));
                            }
                    }
                } else {
                    String moveSource = request
                            .getHeader(TAG.ROCKSTOR_MOVE_SOURCE);

                    if (moveSource != null) {
                        try {
                            moveSource = URLDecoder.decode(moveSource, "UTF-8");
                        } catch (Exception e1) {
                            throw new ProcessException(
                                    StatusCode.ERR400_InvalidArgument);
                        }
                        ReqMoveObject req = new ReqMoveObject();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, contentLength,
                                authorization);
                        req.setRockstorMoveSource(moveSource);

                        return new MoveObjectTask(req, response, request);
                    } else {
                        String copySource = request
                                .getHeader(TAG.ROCKSTOR_COPY_SOURCE);
                        if (copySource != null) {
                            try {
                                copySource = URLDecoder.decode(copySource,
                                        "UTF-8");
                            } catch (Exception e1) {
                                throw new ProcessException(
                                        StatusCode.ERR400_InvalidArgument);
                            }
                            ReqCopyObject req = new ReqCopyObject();
                            req.setBasicAttr(method, url, bucket, object, null,
                                    date, dateString, contentType,
                                    contentLength, authorization);
                            String ifMatch = request
                                    .getHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_MATCH);
                            String ifNoneMatch = request
                                    .getHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_NONE_MATCH);
                            long ifModifiedSince = 0;
                            String ifModifiedSinceString = request
                                    .getHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_MODIFIED_SINCE);
                            if (ifModifiedSinceString != null) {
                                try {
                                    ifModifiedSince = request
                                            .getDateHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_MODIFIED_SINCE);
                                } catch (Exception e) {
                                    LOG.error("Parse head error : "
                                            + TAG.ROCKSTOR_COPY_SOURCE_IF_MODIFIED_SINCE);
                                    throw new ProcessException(
                                            StatusCode.ERR400_MalformedHeaderValue);
                                }
                            }
                            long ifUnmodifiedSince = 0;
                            String ifUnmodifiedSinceString = request
                                    .getHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_UNMODIFIED_SINCE);
                            if (ifUnmodifiedSinceString != null) {
                                try {
                                    ifUnmodifiedSince = request
                                            .getDateHeader(TAG.ROCKSTOR_COPY_SOURCE_IF_UNMODIFIED_SINCE);
                                } catch (Exception e) {
                                    LOG.error("Parse head error : "
                                            + TAG.ROCKSTOR_COPY_SOURCE_IF_UNMODIFIED_SINCE);
                                    throw new ProcessException(
                                            StatusCode.ERR400_MalformedHeaderValue);
                                }
                            }
                            req.setRockstorCopySource(copySource);
                            req.setRockstorCopySourceIfMatch(ifMatch);
                            req.setRockstorCopySourceIfNoneMatch(ifNoneMatch);
                            req.setRockstorCopySourceIfModifiedSince(ifModifiedSince);
                            req.setRockstorCopySourceIfUnmodifiedSince(ifUnmodifiedSince);
                            return new CopyObjectTask(req, response, request);
                        } else {
                            ReqPutObject req = new ReqPutObject();
                            req.setBasicAttr(method, url, bucket, object, null,
                                    date, dateString, contentType,
                                    contentLength, authorization);

                            HashMap<String, String> metas = new HashMap<String, String>();
                            @SuppressWarnings("unchecked")
                            Enumeration<String> names = request
                                    .getHeaderNames();
                            while (names.hasMoreElements()) {
                                String headName = names.nextElement();
                                if (headName.indexOf(TAG.ROCKSTOR_MATA_) == 0)
                                    metas.put(
                                            headName.substring(TAG.ROCKSTOR_META_PREFIX_LENGTH),
                                            request.getHeader(headName));
                            }
                            try {
                                req.setInput(request.getInputStream());
                                req.setRockstorAcl(request
                                        .getHeader(TAG.ROCKSTOR_ACL));
                                req.setMetas(metas);
                            } catch (IOException e) {
                                LOG.error("open ServletInputStream error : "
                                        + ExceptionLogger.getStack(e));
                                throw new ProcessException(
                                        StatusCode.ERR400_IncompleteBody);
                            }

                            return new PutObjectTask(req, response, request);
                        }
                    }
                }
            }

        } else if (method.equals("DELETE")) {
            if (bucket == null && object == null) {
                LOG.error("HEAD request should be apply to a bucket or an object : bucket = "
                        + bucket + ", object = " + object);
                throw new ProcessException(StatusCode.ERR400_InvalidURI);
            } else if (bucket != null && object == null) {
                ReqDelBucket req = new ReqDelBucket();
                req.setBasicAttr(method, url, bucket, object, null, date,
                        dateString, contentType, 0, authorization);
                return new DelBucketTask(req, response, request);
            } else if (bucket != null && object != null) {
                // check if delete meta
                // check if delete meta
                String qStr = request.getQueryString();
                if (qStr != null && qStr.startsWith(TAG.META)) {
                    String[] metas = request.getParameterValues(TAG.META);
                    Set<String> validKeys = new HashSet<String>();
                    if (metas != null) {
                        LOG.info("delete metas, key num: " + metas.length);
                        for (String k : metas) {
                            if (k.isEmpty()) {
                                continue;
                            }
                            validKeys.add(k.trim());
                        }

                        ReqDeleteMeta req = new ReqDeleteMeta();
                        req.setBasicAttr(method, url, bucket, object, null,
                                date, dateString, contentType, 0, authorization);
                        req.setMetas(validKeys.toArray(new String[0]));

                        return new DeleteMetaTask(req, response, request);
                    }
                }

                String uploadId = request.getHeader(TAG.UPLOAD_ID);
                if (uploadId != null) { // abort multipart upload
                    if (uploadId.length() != UploadIdGen.UPLOAD_ID_STR_LEN) {
                        throw new ProcessException(
                                StatusCode.ERR404_NoSuchUpload);
                    }
                    ReqAbortMultiPart req = new ReqAbortMultiPart();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    req.setUploadId(uploadId);

                    return new AbortMultiPartTask(req, response, request);
                } else {
                    ReqDelObject req = new ReqDelObject();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, 0, authorization);
                    return new DelObjectTask(req, response, request);
                }
            }

        } else if (method.equals("HEAD")) {
            if (bucket == null || object == null) {
                LOG.error("HEAD request should be apply to an object : bucket = "
                        + bucket + ", object = " + object);
                throw new ProcessException(StatusCode.ERR400_InvalidURI);
            } else {
                ReqHeadObject req = new ReqHeadObject();
                req.setBasicAttr(method, url, bucket, object, null, date,
                        dateString, contentType, 0, authorization);
                req.setIfMatch(request.getParameter(TAG.IF_MATCH));
                req.setIfModifiedSince(request
                        .getParameter(TAG.IF_MODIFIED_SINCE));
                req.setIfNoneMatch(request.getParameter(TAG.IF_NONE_MATCH));
                req.setIfUnmodifiedSince(request
                        .getParameter(TAG.IF_UNMODIFIED_SINCE));
                return new HeadObjectTask(req, response, request);
            }
        } else if (method.equals("POST")) {

            // parse ContentLength
            String contentLengthString = request.getHeader(TAG.CONTENT_LENGTH);
            if (contentLengthString == null) {
                throw new ProcessException(
                        StatusCode.ERR411_MissingContentLength);
            }
            long contentLength = 0;
            try {
                contentLength = Long.parseLong(contentLengthString);
            } catch (Exception e) {
            }

            if (bucket == null || object == null) {
                LOG.error("HEAD request should be apply to an object : bucket = "
                        + bucket + ", object = " + object);
                throw new ProcessException(StatusCode.ERR400_InvalidURI);
            }

            String uploadId = request.getParameter(TAG.UPLOAD_ID);

            if (uploadId != null) { // complete multipart upload
                if (uploadId.length() != UploadIdGen.UPLOAD_ID_STR_LEN) {
                    throw new ProcessException(StatusCode.ERR404_NoSuchUpload);
                }

                InputStream input = null;
                try {
                    input = request.getInputStream();
                    CompletePartList parts = (CompletePartList) XMLData
                            .deserialize(CompletePartList.class, input);
                    ReqCompleteMultiPart req = new ReqCompleteMultiPart();
                    req.setBasicAttr(method, url, bucket, object, null, date,
                            dateString, contentType, contentLength,
                            authorization);

                    req.setUploadId(uploadId);
                    req.setParts(parts);

                    return new CompleteMultiPartTask(req, response, request);
                } catch (IOException e) {
                    LOG.error("read ServletInputStream catch IOException : "
                            + ExceptionLogger.getStack(e));
                    throw new ProcessException(
                            StatusCode.ERR400_MalformedPartListError);
                } catch (JAXBException e) {
                    e.printStackTrace();
                    LOG.error("parse xml data catch JAXBException : " + e);
                    throw new ProcessException(StatusCode.ERR400_MalformedXML);
                } finally {
                    if (input != null)
                        try {
                            input.close();
                        } catch (IOException e) {
                            LOG.error("close ServletInputStream error : "
                                    + ExceptionLogger.getStack(e));
                        }
                }
            } else { // init multipart upload
                String uploads = request.getParameter(TAG.UPLOADS);
                if (uploads == null) {
                    throw new ProcessException(StatusCode.ERR400_InvalidURI);
                }

                ReqInitMultiPart req = new ReqInitMultiPart();
                req.setBasicAttr(method, url, bucket, object, null, date,
                        dateString, contentType, contentLength, authorization);

                HashMap<String, String> metas = new HashMap<String, String>();
                @SuppressWarnings("unchecked")
                Enumeration<String> names = request.getHeaderNames();
                while (names.hasMoreElements()) {
                    String headName = names.nextElement();
                    if (headName.indexOf(TAG.ROCKSTOR_MATA_) == 0)
                        metas.put(headName
                                .substring(TAG.ROCKSTOR_META_PREFIX_LENGTH),
                                request.getHeader(headName));
                }

                req.setRockstorAcl(request.getHeader(TAG.ROCKSTOR_ACL));
                req.setMetas(metas);
                return new InitMultiPartTask(req, response, request);
            }

        } else {
            LOG.error("It is an unknown HTTP Method : " + method);
            throw new ProcessException(StatusCode.ERR405_MethodNotAllowed);
        }

        // should not get here.
        LOG.error("It is an unknown HTTP Method : " + method);
        return null;
    }
}
