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

package com.rockstor.webifc.req;

public class ReqListMultiParts extends Req {
    private String keyMarker = null;
    private String uploadIdMarker = null;
    private int maxUploads = 1000;
    private String prefix = null;
    private String delimiter = null;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String toString() {
        return "ReqListMultiParts : /" + bucket + ", prefix = " + prefix
                + ", keyMarker = " + keyMarker + ", uploadIdMarker = "
                + uploadIdMarker + ", delimiter = " + delimiter
                + ", maxUploads = " + maxUploads;
    }

    /**
     * @param maxUploads
     *            the maxUploads to set
     */
    public void setMaxUploads(int maxUploads) {
        this.maxUploads = maxUploads;
    }

    /**
     * @return the maxUploads
     */
    public int getMaxUploads() {
        return maxUploads;
    }

    /**
     * @param uploadIdMarker
     *            the uploadIdMarker to set
     */
    public void setUploadIdMarker(String uploadIdMarker) {
        this.uploadIdMarker = uploadIdMarker;
    }

    /**
     * @return the uploadIdMarker
     */
    public String getUploadIdMarker() {
        return uploadIdMarker;
    }

    /**
     * @param keyMarker
     *            the keyMarker to set
     */
    public void setKeyMarker(String keyMarker) {
        this.keyMarker = keyMarker;
    }

    /**
     * @return the keyMarker
     */
    public String getKeyMarker() {
        return keyMarker;
    }

}
