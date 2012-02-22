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

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletInputStream;

public class ReqInitMultiPart extends Req {

    private ServletInputStream input;
    private String rockstorAcl;
    private HashMap<String, String> metas;

    /**
     * @return the input
     */
    public ServletInputStream getInput() {
        return input;
    }

    /**
     * @return the metas
     */
    public HashMap<String, String> getMetas() {
        return metas;
    }

    /**
     * @param metas
     *            the metas to set
     */
    public void setMetas(HashMap<String, String> metas) {
        this.metas = metas;
    }

    /**
     * @param input
     *            the input to set
     */
    public void setInput(ServletInputStream input) {
        this.input = input;
    }

    /**
     * @return the rockstorAcl
     */
    public String getRockstorAcl() {
        return rockstorAcl;
    }

    /**
     * @param rockstorAcl
     *            the rockstorAcl to set
     */
    public void setRockstorAcl(String rockstorAcl) {
        this.rockstorAcl = rockstorAcl;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReqPutObject : /").append(bucket).append(object)
                .append(", rockstorAcl = ").append(rockstorAcl)
                .append(", contentLength=").append(contentLength)
                .append(", contentType").append(contentType);
        for (Map.Entry<String, String> meta : metas.entrySet()) {
            sb.append("Meta[").append(meta.getKey()).append(", ")
                    .append(meta.getValue()).append("]");
        }
        return sb.toString();
    }
}
