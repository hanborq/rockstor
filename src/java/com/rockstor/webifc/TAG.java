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

package com.rockstor.webifc;

public class TAG {

    public final static String HOST = "host";
    public final static String DATE = "Date";
    public final static String AUTHORIZATION = "Authorization";
    public final static String CONTENT_LENGTH = "Content-Length";
    public final static String CONTENT_TYPE = "Content-Type";
    public final static String IF_MATCH = "If-Match";
    public final static String IF_MODIFIED_SINCE = "If-Modified-Since";
    public final static String IF_NONE_MATCH = "If-None-Match";
    public final static String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
    public final static String RANGE = "Range";
    public final static String ETAG = "ETag";
    public final static String ACL = "acl";
    public final static String META = "meta";
    public final static String MARKER = "marker";
    public final static String MAX_KEYS = "max-keys";
    public final static String LAST_MODIFIED = "Last-Modified";

    public final static String PREFIX = "prefix";
    public final static String DELIMITER = "delimiter";

    // for multipart upload
    public final static String UPLOAD_ID = "uploadId";
    public final static String UPLOADS = "uploads";
    public final static String PART_NUMBER = "partNumber";
    public final static String MAX_UPLOADS = "max-uploads";
    public final static String KEY_MARKER = "key-marker";
    public final static String UPLOAD_ID_MARKER = "upload-id-marker";

    public final static String ROCKSTOR_MOVE_SOURCE = "rockstor-move-source";

    public final static String ROCKSTOR_ACL = "rockstor-acl";
    public final static String ROCKSTOR_COPY_SOURCE = "rockstor-copy-source";
    public final static String ROCKSTOR_COPY_SOURCE_IF_MATCH = "rockstor-copy-source-if-match";
    public final static String ROCKSTOR_COPY_SOURCE_IF_NONE_MATCH = "rockstor-copy-source-if-none-match";
    public final static String ROCKSTOR_COPY_SOURCE_IF_MODIFIED_SINCE = "rockstor-copy-source-if-modified-since";
    public final static String ROCKSTOR_COPY_SOURCE_IF_UNMODIFIED_SINCE = "rockstor-copy-source-if-unmodified-since";
    public final static String ROCKSTOR_MATA_ = "rockstor-meta-";

    public final static int ROCKSTOR_META_PREFIX_LENGTH = ROCKSTOR_MATA_
            .length();
}
