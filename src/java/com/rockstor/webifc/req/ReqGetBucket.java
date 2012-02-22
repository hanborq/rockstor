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

public class ReqGetBucket extends Req {

    private String marker = null;
    private int maxKeys = 0;
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

    /**
     * @return the marker
     */
    public String getMarker() {
        return marker;
    }

    /**
     * @param marker
     *            the marker to set
     */
    public void setMarker(String marker) {
        this.marker = marker;
    }

    /**
     * @return the maxKeys
     */
    public int getMaxKeys() {
        return maxKeys;
    }

    /**
     * @param maxKeys
     *            the maxKeys to set
     */
    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }

    @Override
    public String toString() {
        return "ReqGetBucket : /" + bucket + ", marker = " + marker
                + ", prefix = " + prefix + ", delimiter = " + delimiter
                + ", maxKeys = " + maxKeys;
    }

}
