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

package com.rockstor.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class RockStor {

    private static final char[] hexChars = { '0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    private static HashMap<Class, JAXBContext> serializeContextMap = new HashMap<Class, JAXBContext>();
    private static HashMap<Class, JAXBContext> deserializeContextMap = new HashMap<Class, JAXBContext>();
    static {
        try {
            serializeContextMap.put(AccessControlList.class,
                    JAXBContext.newInstance(AccessControlList.class));
            deserializeContextMap.put(AccessControlList.class,
                    JAXBContext.newInstance(AccessControlList.class));
            serializeContextMap.put(AclEntry.class,
                    JAXBContext.newInstance(AclEntry.class));
            deserializeContextMap.put(AclEntry.class,
                    JAXBContext.newInstance(AclEntry.class));
            serializeContextMap.put(Bucket.class,
                    JAXBContext.newInstance(Bucket.class));
            deserializeContextMap.put(Bucket.class,
                    JAXBContext.newInstance(Bucket.class));
            serializeContextMap.put(CommonPrefixes.class,
                    JAXBContext.newInstance(CommonPrefixes.class));
            deserializeContextMap.put(CommonPrefixes.class,
                    JAXBContext.newInstance(CommonPrefixes.class));
            serializeContextMap.put(Contents.class,
                    JAXBContext.newInstance(Contents.class));
            deserializeContextMap.put(Contents.class,
                    JAXBContext.newInstance(Contents.class));
            serializeContextMap.put(Error.class,
                    JAXBContext.newInstance(Error.class));
            deserializeContextMap.put(Error.class,
                    JAXBContext.newInstance(Error.class));
            serializeContextMap.put(ListAllMyBucketsResult.class,
                    JAXBContext.newInstance(ListAllMyBucketsResult.class));
            deserializeContextMap.put(ListAllMyBucketsResult.class,
                    JAXBContext.newInstance(ListAllMyBucketsResult.class));
            serializeContextMap.put(ListBucketResult.class,
                    JAXBContext.newInstance(ListBucketResult.class));
            deserializeContextMap.put(ListBucketResult.class,
                    JAXBContext.newInstance(ListBucketResult.class));
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    private static final String AUTH = "Authorization";
    private static final String DATE = "Date";
    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String ROCKSTOR_ACL = "rockstor-acl";
    private static final String ROCKSTOR_META = "rockstor-meta-";
    private static final String DELIMITER = "delimiter";
    private static final String PREFIX = "prefix";
    private static final String MARKER = "marker";
    private static final String MAX_KEYS = "max-keys";

    public String uri;
    public String accessKey;
    public String securityKey;
    public String AUTH_VERSION;

    public RockStor(String address, String userName) {
        this.uri = "http://" + address;
        this.accessKey = userName;
        this.securityKey = "securityKey";
        AUTH_VERSION = "ROCK0";
    }

    private RockStorException buildExcption(HttpURLConnection httpConnection) {
        InputStream is = null;
        try {
            is = httpConnection.getErrorStream();
            JAXBContext context = deserializeContextMap.get(Error.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            Error error = (Error) unmarshaller.unmarshal(is);
            return new RockStorException(error);
        } catch (Exception e) {
            return buildException(e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    return buildException(e);
                }
                is = null;
            }
        }
    }

    private RockStorException buildException(Exception e) {
        if (e instanceof RockStorException) {
            return (RockStorException) e;
        } else {
            RockStorException rse = new RockStorException();
            rse.initCause(e);
            Error error = new Error();
            error.setCode(e.getClass().getSimpleName());
            error.setMessgae(e.getMessage());
            rse.setError(error);
            return rse;
        }
    }

    public ListAllMyBucketsResult getService() throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            URL url = new URL(uri + "/");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                is = httpConnection.getInputStream();
                JAXBContext context = deserializeContextMap
                        .get(ListAllMyBucketsResult.class);
                Unmarshaller unmarshaller = context.createUnmarshaller();
                ListAllMyBucketsResult result = (ListAllMyBucketsResult) unmarshaller
                        .unmarshal(is);
                return result;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void createBucket(String bucket) throws RockStorException {
        createBucket(bucket, null);
    }

    public void createBucket(String bucket, BucketDefaultAcl acl)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            URL url = new URL(uri + "/" + bucket);
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("PUT", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.setFixedLengthStreamingMode(0);
            if (acl != null)
                httpConnection.setRequestProperty(ROCKSTOR_ACL,
                        BucketDefaultAcl.getAclString(acl));
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void deleteBucket(String bucket) throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            URL url = new URL(uri + "/" + bucket);
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("DELETE");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("DELETE", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 204) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public AccessControlList getBucketAcl(String bucket)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            URL url = new URL(uri + "/" + bucket + "?acl");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                is = httpConnection.getInputStream();
                JAXBContext context = deserializeContextMap
                        .get(AccessControlList.class);
                Unmarshaller unmarshaller = context.createUnmarshaller();
                AccessControlList result = (AccessControlList) unmarshaller
                        .unmarshal(is);
                return result;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void setBucketAcl(String bucket, AccessControlList acl)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        OutputStream os = null;
        try {
            URL url = new URL(uri + "/" + bucket + "?acl");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("PUT", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            os = httpConnection.getOutputStream();
            JAXBContext context = serializeContextMap
                    .get(AccessControlList.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.marshal(acl, os);
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (os != null)
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public ListBucketResult getBucket(String bucket) throws RockStorException {
        return getBucket(bucket, null, null, null, 0);
    }

    public ListBucketResult getBucket(String bucket, String prefix,
            String delimiter, String marker, int maxKeys)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            String param = "";
            if (prefix != null) {
                param = "prefix=" + prefix;
            }
            if (delimiter != null) {
                if (param.equals(""))
                    param = "delimiter=" + delimiter;
                else
                    param += "&delimiter=" + delimiter;
            }
            if (marker != null) {
                if (param.equals(""))
                    param = "marker=" + marker;
                else
                    param += "&marker=" + marker;
            }
            if ((maxKeys <= 200) && (maxKeys > 0)) {
                if (param.equals(""))
                    param = "max-keys=" + maxKeys;
                else
                    param += "&max-keys=" + maxKeys;
            }

            URL url = new URL(uri + "/" + bucket
                    + (param.equals("") ? "" : ("?" + param)));
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            if (prefix != null)
                httpConnection.setRequestProperty(PREFIX, prefix);

            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                is = httpConnection.getInputStream();
                JAXBContext context = deserializeContextMap
                        .get(ListBucketResult.class);
                Unmarshaller unmarshaller = context.createUnmarshaller();
                ListBucketResult result = (ListBucketResult) unmarshaller
                        .unmarshal(is);
                return result;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void putObject(String bucket, String object, ObjectDefaultAcl acl,
            Map<String, String> metas, String contentType, int contentLength,
            InputStream is) throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        OutputStream os = null;

        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object));
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("PUT", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.setFixedLengthStreamingMode(contentLength);
            //
            // httpConnection.setRequestProperty(CONTENT_LENGTH,
            // String.valueOf(contentLength));
            if (contentType != null)
                httpConnection.setRequestProperty(CONTENT_TYPE, contentType);
            if (acl != null)
                httpConnection.setRequestProperty(ROCKSTOR_ACL,
                        ObjectDefaultAcl.getAclString(acl));
            if (metas != null) {
                for (Map.Entry<String, String> e : metas.entrySet()) {
                    if (e.getKey().indexOf("ROCKSTOR_META") == 0)
                        httpConnection.setRequestProperty(e.getKey(),
                                e.getValue());
                }
            }
            httpConnection.connect();
            os = httpConnection.getOutputStream();
            writeData(is, os);
            os.flush();

            int code = httpConnection.getResponseCode();
            if (code == 200) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (os != null)
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public InputStream getObject(String bucket, String object)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object));
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                is = httpConnection.getInputStream();
                writeData(is, bos);
                return new ByteArrayInputStream(bos.toByteArray());
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException ie) {
                    ie.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void deleteObject(String bucket, String object)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object));
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("DELETE");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("DELETE", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 204) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public Map<String, String> headObject(String bucket, String object)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object));
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("HEAD");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("HEAD", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                HashMap<String, String> map = new HashMap<String, String>();
                Map<String, List<String>> retHeads = httpConnection
                        .getHeaderFields();
                for (Map.Entry<String, List<String>> e : retHeads.entrySet()) {
                    String key = e.getKey();
                    if (key != null
                            && (key.equals("ETag")
                                    || key.equals(CONTENT_LENGTH)
                                    || key.equals("Last-Modified")
                                    || key.equals(CONTENT_TYPE) || key
                                    .indexOf(ROCKSTOR_META) == 0)) {
                        map.put(key, e.getValue().get(0));
                    }
                }
                return map;
            } else {
                Error error = new Error();
                error.setCode("Head Error");
                error.setMessgae("Code " + code);
                throw new RockStorException(error);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public AccessControlList getObjectAcl(String bucket, String object)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        InputStream is = null;
        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object)
                    + "?acl");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                is = httpConnection.getInputStream();
                JAXBContext context = deserializeContextMap
                        .get(AccessControlList.class);
                Unmarshaller unmarshaller = context.createUnmarshaller();
                AccessControlList result = (AccessControlList) unmarshaller
                        .unmarshal(is);
                return result;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void setObjectAcl(String bucket, String object, AccessControlList acl)
            throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        OutputStream os = null;

        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object)
                    + "?acl");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("PUT", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            os = httpConnection.getOutputStream();
            JAXBContext context = serializeContextMap
                    .get(AccessControlList.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.marshal(acl, os);
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (os != null)
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void setObjectMeta(String bucket, String object,
            Map<String, String> metas) throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            URL url = new URL(uri + "/" + bucket + "/" + getObjectInUrl(object)
                    + "?meta");
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("PUT", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.setFixedLengthStreamingMode(0);
            if (metas != null) {
                for (Map.Entry<String, String> e : metas.entrySet()) {
                    String key = e.getKey();
                    if (key != null && key.indexOf(ROCKSTOR_META) == 0)
                        httpConnection.setRequestProperty(key, e.getValue());
                }
            }
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public void deleteObjectMeta(String bucket, String object,
            List<String> metas) throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            String params = "";
            if (metas != null) {
                for (int i = 0; i < metas.size(); i++) {
                    if (metas.get(i).indexOf(ROCKSTOR_META) == 0) {
                        if (!params.equals("")) {
                            params += "&";
                        }
                        params += "meta="
                                + metas.get(i)
                                        .substring(ROCKSTOR_META.length());
                    }
                }
            } else {
                params = "meta";
            }
            String urlString = uri + "/" + bucket + "/"
                    + getObjectInUrl(object) + "?" + params;
            URL url = new URL(urlString);
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("DELETE");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestProperty(AUTH, getAuth("DELETE", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 204) {
                return;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    public Map<String, String> getObjectMeta(String bucket, String object,
            List<String> metas) throws RockStorException {
        String date = getDate();
        HttpURLConnection httpConnection = null;
        try {
            String params = "";
            if (metas != null) {
                for (int i = 0; i < metas.size(); i++) {
                    if (metas.get(i).indexOf(ROCKSTOR_META) == 0) {
                        if (!params.equals("")) {
                            params += "&";
                        }
                        params += "meta="
                                + metas.get(i)
                                        .substring(ROCKSTOR_META.length());
                    }
                }
            } else {
                params = "meta";
            }
            String urlString = uri + "/" + bucket + "/"
                    + getObjectInUrl(object) + "?" + params;
            URL url = new URL(urlString);
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);
            httpConnection.setRequestMethod("GET");
            httpConnection.setRequestProperty(AUTH, getAuth("GET", date));
            httpConnection.setRequestProperty(DATE, date);
            httpConnection.connect();
            int code = httpConnection.getResponseCode();
            if (code == 200) {
                HashMap<String, String> map = new HashMap<String, String>();
                Map<String, List<String>> retHeads = httpConnection
                        .getHeaderFields();
                for (Map.Entry<String, List<String>> e : retHeads.entrySet()) {
                    String key = e.getKey();
                    if (key != null && key.indexOf(ROCKSTOR_META) == 0) {
                        map.put(key, e.getValue().get(0));
                    }
                }
                return map;
            } else {
                throw buildExcption(httpConnection);
            }
        } catch (Exception e) {
            throw buildException(e);
        } finally {
            if (httpConnection != null)
                httpConnection.disconnect();
        }
    }

    private String getDate() {
        SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = new Date();
        return sdf.format(date);
    }

    private String getAuth(String method, String date) {
        String sig = method + date + securityKey;
        byte[] sigbytes = sig.getBytes();
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md.update(sigbytes, 0, sigbytes.length);
        byte[] digs = md.digest();
        String hex = "";
        int msb;
        int lsb = 0;
        int i;
        // MSB maps to idx 0
        for (i = 0; i < digs.length; i++) {
            msb = (digs[i] & 0x000000FF) / 16;
            lsb = (digs[i] & 0x000000FF) % 16;
            hex = hex + hexChars[msb] + hexChars[lsb];
        }
        return AUTH_VERSION + " " + accessKey + ":" + (hex);
    }

    private String getObjectInUrl(String path) {
        String[] paths = path.split("/");
        String objectRebuild = "";
        for (int i = 0; i < paths.length; i++) {
            if (i > 0)
                objectRebuild += "/";
            try {
                objectRebuild += URLEncoder.encode(paths[i], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return objectRebuild;
    }

    private void writeData(InputStream is, OutputStream os) throws IOException {
        byte[] b = new byte[4096];
        int len = 0;
        while ((len = is.read(b)) > 0) {
            os.write(b, 0, len);
        }
    }

}
