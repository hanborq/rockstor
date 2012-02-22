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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class RockStorSender {

    public static HttpResp perform(HttpReq req) throws URISyntaxException,
            ClientProtocolException, IOException {

        HttpResp resp = null;
        HttpRequestBase request = buildHttpRequest(req);
        DefaultHttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(request);
        resp = buildHttpResponse(response);

        return resp;
    }

    private static HttpRequestBase buildHttpRequest(HttpReq req)
            throws UnsupportedEncodingException, URISyntaxException {

        HttpRequestBase request = null;
        if ("GET".equals(req.getMethod())) {
            request = new HttpGet();
        } else if ("PUT".equals(req.getMethod())) {
            request = new HttpPut();
            if (req.getBody() != null && req.getBody().length() > 0)
                ((HttpPut) request).setEntity(new StringEntity(req.getBody()));
        } else if ("DELETE".equals(req.getMethod())) {
            request = new HttpDelete();
        } else if ("HEAD".equals(req.getMethod())) {
            request = new HttpHead();
        } else {
            throw new NullPointerException("Unknown HTTP Method : "
                    + req.getMethod());
        }

        request.setURI(new URI(req.getUrl()));

        if (req.getHead() != null) {
            for (Map.Entry<String, String> e : req.getHead().entrySet()) {
                if ("PUT".equals(req.getMethod())
                        && e.getKey().equals("Content-Length"))
                    continue;
                request.setHeader(e.getKey(), e.getValue());
            }
        }

        return request;
    }

    private static HttpResp buildHttpResponse(HttpResponse response) {
        HttpResp resp = new HttpResp();
        resp.setStatus(response.getStatusLine().getStatusCode());
        Map<String, String> head = new HashMap<String, String>();
        for (Header header : response.getAllHeaders()) {
            head.put(header.getName(), header.getValue());
        }
        resp.setHead(head);

        if ("chunked".equals(head.get("Transfer-Encoding"))
                || (head.containsKey("Content-Length") && !head.get(
                        "Content-Length").equals("0"))) {
            try {
                InputStream is = response.getEntity().getContent();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int len = 0;
                byte[] b = new byte[4096];
                while ((len = is.read(b)) > 0) {
                    baos.write(b, 0, len);
                }
                baos.flush();
                resp.setBody(new String(baos.toByteArray()));
            } catch (IllegalStateException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (NullPointerException e) {
                // HEAD ?
            }
        }
        return resp;
    }

    public static void main(String[] args) throws ClientProtocolException,
            URISyntaxException, IOException {

        HttpReq req = new HttpReq();
        req.setMethod("GET");
        req.setUrl("http://10.24.1.252:8080/rockstor/");
        req.setHead(new HashMap<String, String>());
        HttpResp resp = perform(req);
        System.out.println(resp.getStatus());
        System.out.println(resp.getBody());
    }

}
