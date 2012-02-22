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

public class ReqCopyObject extends Req {

    private String rockstorCopySource;
    private String rockstorCopySourceIfMatch;
    private String rockstorCopySourceIfNoneMatch;
    private long rockstorCopySourceIfModifiedSince;
    private long rockstorCopySourceIfUnmodifiedSince;

    /**
     * @return the rockstorCopySource
     */
    public String getRockstorCopySource() {
        return rockstorCopySource;
    }

    /**
     * @param rockstorCopySource
     *            the rockstorCopySource to set
     */
    public void setRockstorCopySource(String rockstorCopySource) {
        this.rockstorCopySource = rockstorCopySource;
    }

    /**
     * @return the rockstorCopySourceIfMatch
     */
    public String getRockstorCopySourceIfMatch() {
        return rockstorCopySourceIfMatch;
    }

    /**
     * @param rockstorCopySourceIfMatch
     *            the rockstorCopySourceIfMatch to set
     */
    public void setRockstorCopySourceIfMatch(String rockstorCopySourceIfMatch) {
        this.rockstorCopySourceIfMatch = rockstorCopySourceIfMatch;
    }

    /**
     * @return the rockstorCopySourceIfNoneMatch
     */
    public String getRockstorCopySourceIfNoneMatch() {
        return rockstorCopySourceIfNoneMatch;
    }

    /**
     * @param rockstorCopySourceIfNoneMatch
     *            the rockstorCopySourceIfNoneMatch to set
     */
    public void setRockstorCopySourceIfNoneMatch(
            String rockstorCopySourceIfNoneMatch) {
        this.rockstorCopySourceIfNoneMatch = rockstorCopySourceIfNoneMatch;
    }

    /**
     * @return the rockstorCopySourceIfModifiedSince
     */
    public long getRockstorCopySourceIfModifiedSince() {
        return rockstorCopySourceIfModifiedSince;
    }

    /**
     * @param rockstorCopySourceIfModifiedSince
     *            the rockstorCopySourceIfModifiedSince to set
     */
    public void setRockstorCopySourceIfModifiedSince(
            long rockstorCopySourceIfModifiedSince) {
        this.rockstorCopySourceIfModifiedSince = rockstorCopySourceIfModifiedSince;
    }

    /**
     * @return the rockstorCopySourceIfUnmodifiedSince
     */
    public long getRockstorCopySourceIfUnmodifiedSince() {
        return rockstorCopySourceIfUnmodifiedSince;
    }

    /**
     * @param rockstorCopySourceIfUnmodifiedSince
     *            the rockstorCopySourceIfUnmodifiedSince to set
     */
    public void setRockstorCopySourceIfUnmodifiedSince(
            long rockstorCopySourceIfUnmodifiedSince) {
        this.rockstorCopySourceIfUnmodifiedSince = rockstorCopySourceIfUnmodifiedSince;
    }

    @Override
    public String toString() {
        return "ReqCopyObject : /" + bucket + object
                + ", rockstorCopySource = " + rockstorCopySource
                + ", rockstorCopySourceIfMatch = " + rockstorCopySourceIfMatch
                + ", rockstorCopySourceIfNoneMatch = "
                + rockstorCopySourceIfNoneMatch
                + ", rockstorCopySourceIfModifiedSince = "
                + rockstorCopySourceIfModifiedSince
                + ", rockstorCopySourceIfUnmodifiedSince = "
                + rockstorCopySourceIfUnmodifiedSince;
    }

}
