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

import javax.servlet.ServletInputStream;

public class ReqUploadPart extends Req {
    private String uploadId;
    private short partNumber;
    private ServletInputStream input;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public short getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(short partNumber) {
        this.partNumber = partNumber;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReqUploadPart : /").append(bucket).append(object)
                .append(", uploadId = ").append(uploadId)
                .append(", partNumber").append(partNumber)
                .append(", contentLength=").append(contentLength);
        return sb.toString();
    }

    /**
     * @param input
     *            the input to set
     */
    public void setInput(ServletInputStream input) {
        this.input = input;
    }

    /**
     * @return the input
     */
    public ServletInputStream getInput() {
        return input;
    }

}
