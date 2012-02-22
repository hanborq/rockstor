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

public class ReqGetMeta extends Req {
    private String[] metas;

    /**
     * @return the metas
     */
    public String[] getMetas() {
        return metas;
    }

    /**
     * @param metas
     *            the metas to set
     */
    public void setMetas(String[] metas) {
        this.metas = metas;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReqGetMeta : /").append(bucket).append(object);
        sb.append(", Meta[");
        for (String meta : metas) {
            sb.append(meta).append(", ");
        }
        sb.append("]");
        return sb.toString();
    }
}