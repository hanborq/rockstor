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

public class ReqListParts extends Req {
    private String uploadId = null;
    private short maxParts = 1000;
    private short partNumberMarker = 0;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReqListParts : /").append(bucket).append(object)
                .append(", uploadId = ").append(uploadId)
                .append(", max-parts = ").append(maxParts)
                .append(", part-number-marker = ").append(partNumberMarker);
        return sb.toString();
    }

    /**
     * @param uploadId
     *            the uploadId to set
     */
    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    /**
     * @return the uploadId
     */
    public String getUploadId() {
        return uploadId;
    }

    /**
     * @param partNumberMarker
     *            the partNumberMarker to set
     */
    public void setPartNumberMarker(short partNumberMarker) {
        this.partNumberMarker = partNumberMarker;
    }

    /**
     * @return the partNumberMarker
     */
    public short getPartNumberMarker() {
        return partNumberMarker;
    }

    /**
     * @param maxParts
     *            the maxParts to set
     */
    public void setMaxParts(short maxParts) {
        this.maxParts = maxParts;
    }

    /**
     * @return the maxParts
     */
    public short getMaxParts() {
        return maxParts;
    }
}
