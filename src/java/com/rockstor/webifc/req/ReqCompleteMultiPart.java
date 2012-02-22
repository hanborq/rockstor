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

import com.rockstor.webifc.data.CompletePartList;

public class ReqCompleteMultiPart extends Req {
    private String uploadId;
    private CompletePartList parts;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReqCompleteMultiPart : /").append(bucket).append(object)
                .append(", uploadId = ").append(uploadId)
                .append(", completeParts={").append(parts).append("}");

        return sb.toString();
    }

    /**
     * @param parts
     *            the parts to set
     */
    public void setParts(CompletePartList parts) {
        this.parts = parts;
    }

    /**
     * @return the parts
     */
    public CompletePartList getParts() {
        return parts;
    }

}
