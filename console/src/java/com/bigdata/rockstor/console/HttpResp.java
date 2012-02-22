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

package com.bigdata.rockstor.console;

import java.util.Map;

public class HttpResp {

    private int status;
    private Map<String, String> head;
    private String body;

    /**
     * @return the status
     */
    public int getStatus() {
        return status;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return the head
     */
    public Map<String, String> getHead() {
        return head;
    }

    /**
     * @param head
     *            the head to set
     */
    public void setHead(Map<String, String> head) {
        this.head = head;
    }

    /**
     * @return the body
     */
    public String getBody() {
        return body;
    }

    /**
     * @param body
     *            the body to set
     */
    public void setBody(String body) {
        this.body = body;
    }

    public static HttpResp buildConsoleError(String error) {
        HttpResp resp = new HttpResp();
        resp.setStatus(600);
        resp.setBody(error);
        return resp;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Resp : status = " + status + ", heads = {");
        for (Map.Entry<String, String> e : head.entrySet()) {
            sb.append(e.getKey() + " : " + e.getValue() + ", ");
        }
        sb.append("}, body = " + body);
        return sb.toString();
    }
}
