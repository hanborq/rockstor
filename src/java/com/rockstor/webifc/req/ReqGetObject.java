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

public class ReqGetObject extends Req {

    private String ifMatch;
    private String ifModifiedSince;
    private String ifNoneMatch;
    private String ifUnmodifiedSince;

    private boolean fromWeb = false;

    /**
     * @return the ifMatch
     */
    public String getIfMatch() {
        return ifMatch;
    }

    /**
     * @param ifMatch
     *            the ifMatch to set
     */
    public void setIfMatch(String ifMatch) {
        this.ifMatch = ifMatch;
    }

    /**
     * @return the ifModifiedSince
     */
    public String getIfModifiedSince() {
        return ifModifiedSince;
    }

    /**
     * @param ifModifiedSince
     *            the ifModifiedSince to set
     */
    public void setIfModifiedSince(String ifModifiedSince) {
        this.ifModifiedSince = ifModifiedSince;
    }

    /**
     * @return the ifNoneMatch
     */
    public String getIfNoneMatch() {
        return ifNoneMatch;
    }

    /**
     * @param ifNoneMatch
     *            the ifNoneMatch to set
     */
    public void setIfNoneMatch(String ifNoneMatch) {
        this.ifNoneMatch = ifNoneMatch;
    }

    /**
     * @return the ifUnmodifiedSince
     */
    public String getIfUnmodifiedSince() {
        return ifUnmodifiedSince;
    }

    /**
     * @param ifUnmodifiedSince
     *            the ifUnmodifiedSince to set
     */
    public void setIfUnmodifiedSince(String ifUnmodifiedSince) {
        this.ifUnmodifiedSince = ifUnmodifiedSince;
    }

    /**
     * @return the fromWeb
     */
    public boolean isFromWeb() {
        return fromWeb;
    }

    /**
     * @param fromWeb
     *            the fromWeb to set
     */
    public void setFromWeb(boolean fromWeb) {
        this.fromWeb = fromWeb;
    }

    @Override
    public String toString() {
        return "ReqGetObject : /" + bucket + object + ", ifMatch = " + ifMatch
                + ", ifModifiedSince = " + ifModifiedSince + ", ifNoneMatch = "
                + ifNoneMatch + ", ifUnmodifiedSince = " + ifUnmodifiedSince
                + ", fromWeb = " + fromWeb;
    }
}
