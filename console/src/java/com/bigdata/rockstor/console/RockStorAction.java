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

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.opensymphony.xwork2.ActionSupport;

public class RockStorAction extends ActionSupport {

    public static Logger LOG = Logger.getLogger(RockStorAction.class);

    private String httpReq;
    private HttpResp resp;

    /**
     * @return the httpResp
     */
    public HttpResp getHttpResp() {
        return resp;
    }

    /**
     * @param httpReq
     *            the httpReq to set
     */
    public void setHttpReq(String httpReq) {
        this.httpReq = httpReq;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.opensymphony.xwork2.ActionSupport#execute()
     */
    @Override
    public String execute() throws Exception {

        HttpReq req = null;
        try {
            req = parseHttpReq();
            resp = RockStorSender.perform(req);
        } catch (Exception e) {
            LOG.error("Parse Error : " + e);
            e.printStackTrace();
            resp = HttpResp.buildConsoleError(e.getMessage());
        }
        LOG.info(resp);
        return SUCCESS;
    }

    private HttpReq parseHttpReq() throws JsonParseException,
            JsonMappingException, IOException {
        if (httpReq == null)
            throw new NullPointerException("httpReq == null");

        LOG.info("Req : " + httpReq);
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(httpReq, Map.class);

        HttpReq req = new HttpReq();

        req.setMethod((String) map.get("method"));
        req.setHead((Map) map.get("head"));
        req.setBody((String) map.get("body"));

        String url = (String) map.get("url");
        // if (url.equals(System.getProperty("rest.site"))) {
        // req.setUrl((String)map.get("url"));
        // } else {
        // String urlCode =
        // url.substring(System.getProperty("rest.site").length());
        // String[] paths = urlCode.split("/");
        // StringBuilder sb = new StringBuilder();
        // for (int i=0; i < paths.length; i++) {
        // sb.append(URLEncoder.encode(paths[i], "UTF-8"));
        // if (i < (paths.length-1)) {
        // sb.append("/");
        // }
        // }
        // url = System.getProperty("rest.site")+sb.toString();
        // req.setUrl(url);
        // }
        req.setUrl((String) map.get("url"));
        LOG.info("URL : " + url);
        return req;
    }
}
