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

package com.rockstor.util;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;

import com.rockstor.webifc.data.XMLData;

public class StatusCode {

    private static Logger LOG = Logger.getLogger(StatusCode.class);

    public static final String ERR408_TIMEOUT = "Timeout";
    public static final String ERR400_ExcessHeaderValues = "ExcessHeaderValues";
    public static final String ERR400_InvalidArgument = "InvalidArgument";
    public static final String ERR400_InvalidURI = "InvalidURI";
    public static final String ERR400_MalformedHeaderValue = "MalformedHeaderValue";
    public static final String ERR400_MalformedXML = "MalformedXML";
    public static final String ERR400_MaxMessageLengthExceeded = "MaxMessageLengthExceeded";
    public static final String ERR400_MissingRequestBodyError = "MissingRequestBodyError";
    public static final String ERR400_MissingSecurityHeader = "MissingSecurityHeader";
    public static final String ERR400_UnresolvableGrantByEmailAddress = "UnresolvableGrantByEmailAddress";
    public static final String ERR400_InvalidBucketName = "InvalidBucketName";
    public static final String ERR400_TooManyBuckets = "TooManyBuckets";
    public static final String ERR400_UnsupportedAcl = "UnsupportedAcl";
    public static final String ERR400_MalformedACLError = "MalformedACLError";
    public static final String ERR400_BadDigest = "BadDigest";
    public static final String ERR400_EntityTooSmall = "EntityTooSmall";
    public static final String ERR400_EntityTooLarge = "EntityTooLarge";
    public static final String ERR400_IncompleteBody = "IncompleteBody";
    public static final String ERR400_InvalidDigest = "InvalidDigest";
    public static final String ERR400_KeyTooLong = "KeyTooLong";
    public static final String ERR400_MetadataTooLarge = "MetadataTooLarge";

    public static final String ERR400_MalformedPartListError = "MalformedPartListError";
    public static final String ERR400_InvalidPart = "InvalidPart";
    public static final String ERR400_InvalidPartOrder = "InvalidPartOrder";
    public static final String ERR404_NoSuchUpload = "NoSuchUpload";

    public static final String ERR403_AccessDenied = "AccessDenied";
    public static final String ERR403_AccountProblem = "AccountProblem";
    public static final String ERR403_InsufficientQuota = "InsufficientQuota";
    public static final String ERR403_InvalidAccessKeyId = "InvalidAccessKeyId";
    public static final String ERR403_InvalidSecurity = "InvalidSecurity";
    public static final String ERR403_RequestTimeTooSkewed = "RequestTimeTooSkewed";
    public static final String ERR403_SignatureDoesNotMatch = "SignatureDoesNotMatch";

    public static final String ERR404_NoSuchBucket = "NoSuchBucket";
    public static final String ERR404_NoSuchKey = "NoSuchKey";

    public static final String ERR405_MethodNotAllowed = "MethodNotAllowed";

    public static final String ERR409_BucketAlreadyOwnedByYou = "BucketAlreadyOwnedByYou";
    public static final String ERR409_BucketNameUnavailable = "BucketNameUnavailable";
    public static final String ERR409_BucketNotEmpty = "BucketNotEmpty";

    public static final String ERR411_MissingContentLength = "MissingContentLength";

    public static final String ERR412_PreconditionFailed = "PreconditionFailed";

    public static final String ERR416_InvalidRange = "InvalidRange";

    public static final String ERR500_InternalError = "InternalError";

    private static HashMap<String, Integer> codeMap = new HashMap<String, Integer>();
    private static HashMap<String, String> descMap = new HashMap<String, String>();

    public static void init() {
        codeMap.put(ERR408_TIMEOUT, 408);
        codeMap.put(ERR400_ExcessHeaderValues, 400);
        codeMap.put(ERR400_InvalidArgument, 400);
        codeMap.put(ERR400_InvalidURI, 400);
        codeMap.put(ERR400_MalformedHeaderValue, 400);
        codeMap.put(ERR400_MalformedXML, 400);
        codeMap.put(ERR400_MaxMessageLengthExceeded, 400);
        codeMap.put(ERR400_MissingRequestBodyError, 400);
        codeMap.put(ERR400_MissingSecurityHeader, 400);
        codeMap.put(ERR400_UnresolvableGrantByEmailAddress, 400);
        codeMap.put(ERR400_InvalidBucketName, 400);
        codeMap.put(ERR400_TooManyBuckets, 400);
        codeMap.put(ERR400_UnsupportedAcl, 400);
        codeMap.put(ERR400_MalformedACLError, 400);
        codeMap.put(ERR400_BadDigest, 400);
        codeMap.put(ERR400_EntityTooSmall, 400);
        codeMap.put(ERR400_EntityTooLarge, 400);
        codeMap.put(ERR400_IncompleteBody, 400);
        codeMap.put(ERR400_InvalidDigest, 400);
        codeMap.put(ERR400_KeyTooLong, 400);
        codeMap.put(ERR400_MetadataTooLarge, 400);
        codeMap.put(ERR403_AccessDenied, 403);
        codeMap.put(ERR403_AccountProblem, 403);
        codeMap.put(ERR403_InsufficientQuota, 403);
        codeMap.put(ERR403_InvalidAccessKeyId, 403);
        codeMap.put(ERR403_InvalidSecurity, 403);
        codeMap.put(ERR403_RequestTimeTooSkewed, 403);
        codeMap.put(ERR403_SignatureDoesNotMatch, 403);
        codeMap.put(ERR404_NoSuchBucket, 404);
        codeMap.put(ERR404_NoSuchKey, 404);
        codeMap.put(ERR405_MethodNotAllowed, 405);
        codeMap.put(ERR409_BucketAlreadyOwnedByYou, 409);
        codeMap.put(ERR409_BucketNameUnavailable, 409);
        codeMap.put(ERR409_BucketNotEmpty, 409);
        codeMap.put(ERR411_MissingContentLength, 411);
        codeMap.put(ERR412_PreconditionFailed, 412);
        codeMap.put(ERR416_InvalidRange, 416);
        codeMap.put(ERR500_InternalError, 500);

        codeMap.put(ERR400_InvalidPart, 400);
        codeMap.put(ERR400_InvalidPartOrder, 400);
        codeMap.put(ERR404_NoSuchUpload, 404);
        codeMap.put(ERR400_MalformedPartListError, 400);

        descMap.put(ERR408_TIMEOUT,
                "Your request is timeout, maybe the server is busy or the network is too slow.");
        descMap.put(ERR400_ExcessHeaderValues,
                "Multiple HTTP header values where one was expected.");
        descMap.put(ERR400_InvalidArgument, "Invalid argument.");
        descMap.put(ERR400_InvalidURI, "Couldn't parse the specified URI.");
        descMap.put(ERR400_MalformedHeaderValue,
                "An HTTP header value was malformed.");
        descMap.put(
                ERR400_MalformedXML,
                "This happens when the user sends a malformed XML (XML that doesn't conform to the published XSD) for the configuration.");
        descMap.put(ERR400_MaxMessageLengthExceeded,
                "Your request was too big.");
        descMap.put(ERR400_MissingRequestBodyError,
                "This happens when the user sends an empty XML document as a request.");
        descMap.put(ERR400_MissingSecurityHeader,
                "Your request was missing a required header.");
        descMap.put(ERR400_UnresolvableGrantByEmailAddress,
                "The e-mail address you provided does not match any account on record.");
        descMap.put(ERR400_InvalidBucketName,
                "The specified bucket is not valid.");
        descMap.put(ERR400_TooManyBuckets,
                "You have attempted to create more buckets than allowed.");
        descMap.put(
                ERR400_UnsupportedAcl,
                "The ACL you specified is not supported. For more information about the ACLs that Bigdata RockStor Storage supports, seeAccess Control in the Developer's Guide.");
        descMap.put(
                ERR400_MalformedACLError,
                "The ACL you specified is not supported. For more information about the ACLs that Bigdata RockStor supports, seeAccess Control in the Developer's Guide.");
        descMap.put(ERR400_BadDigest,
                "The Content-MD5 you specified did not match what we received.");
        descMap.put(ERR400_EntityTooSmall,
                "Your proposed upload is smaller than the minimum allowed object size.");
        descMap.put(ERR400_EntityTooLarge,
                "Your proposed upload exceeds the maximum allowed object size.");
        descMap.put(
                ERR400_IncompleteBody,
                "You did not provide the number of bytes specified by the Content-Length HTTP header.");
        descMap.put(ERR400_InvalidDigest,
                "The Content-MD5 you specified was invalid.");
        descMap.put(ERR400_KeyTooLong, "Your object name is too long.");
        descMap.put(ERR400_MetadataTooLarge,
                "Your metadata headers exceed the maximum allowed metadata size.");
        descMap.put(ERR403_AccessDenied, "Access denied.");
        descMap.put(
                ERR403_AccountProblem,
                "There is a problem with your Bigdata account that prevents the operation from completing successfully. Please contact customer service.");
        descMap.put(ERR403_InsufficientQuota,
                "The user does not have enough quota to complete this operation.");
        descMap.put(ERR403_InvalidAccessKeyId,
                "The User Id you provided does not exist in our records.");
        descMap.put(ERR403_InvalidSecurity,
                "The provided security credentials are not valid.");
        descMap.put(ERR403_RequestTimeTooSkewed,
                "The difference between the request time and the server's time is too large.");
        descMap.put(
                ERR403_SignatureDoesNotMatch,
                "The request signature we calculated does not match the signature you provided. Check your Bigdata secret and signing method.");
        descMap.put(ERR404_NoSuchBucket, "The specified bucket does not exist.");
        descMap.put(ERR404_NoSuchKey,
                "The specified object name does not exist.");
        descMap.put(ERR405_MethodNotAllowed,
                "The specified method is not allowed against this resource.");
        descMap.put(
                ERR409_BucketAlreadyOwnedByYou,
                "Your previous request to create the named bucket succeeded and you already own it.");
        descMap.put(
                ERR409_BucketNameUnavailable,
                "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again. This error can occur when you try to create a bucket name that already exists or you try to create a bucket name that contains a domain name that does not fall under a recognized top-level domain.");
        descMap.put(ERR409_BucketNotEmpty,
                "The bucket you tried to delete is not empty.");
        descMap.put(ERR411_MissingContentLength,
                "You must provide the Content-Length HTTP header.");
        descMap.put(ERR412_PreconditionFailed,
                "At least one of the pre-conditions you specified did not hold.");
        descMap.put(ERR416_InvalidRange,
                "The requested range cannot be satisfied.");
        descMap.put(ERR500_InternalError,
                "We encountered an internal error. Please try again.");

        descMap.put(
                ERR400_InvalidPart,
                "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.");
        descMap.put(
                ERR400_InvalidPartOrder,
                "The list of parts was not in ascending order. Parts list must specified in order by part number.");
        descMap.put(
                ERR404_NoSuchUpload,
                "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.");
        descMap.put(
                ERR400_MalformedPartListError,
                "The upload part list you specified is not supported. . For more information about the ACLs that Bigdata RockStor supports, seeAccess Control in the Developer's Guide.");
    }

    public static void sendErrorStatusCode(String status,
            HttpServletResponse resp) throws IOException {

        int code = codeMap.get(status);
        String desc = descMap.get(status);

        com.rockstor.webifc.data.Error error = new com.rockstor.webifc.data.Error();
        error.setCode(status);
        error.setMessgae(desc);

        ServletOutputStream output = resp.getOutputStream();

        resp.setStatus(code);

        try {
            XMLData.serialize(error, output);
        } catch (JAXBException e) {
            LOG.error("Send Error catch JAXBException : " + e);
            e.printStackTrace();
        }

        output.close();
    }
}
