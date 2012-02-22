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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

public class UploadServlet extends HttpServlet {

    public static Logger LOG = Logger.getLogger(UploadServlet.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest
     * , javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        if (!ServletFileUpload.isMultipartContent(req)) {
            LOG.error("It is not a MultipartContent, return error.");
            resp.sendError(500, "It is not a MultipartContent, return error.");
            return;
        }

        FileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);
        upload.setFileSizeMax(1024 * 1024 * 512);
        List<FileItem> fileItems = null;
        try {
            fileItems = upload.parseRequest(req);
            LOG.info("parse requeset success : items num : " + fileItems.size());
        } catch (FileUploadException e) {
            LOG.error("parse requeset failed !");
            resp.sendError(500, "parse requeset failed !");
            return;
        }

        HashMap<String, String> headMap = new HashMap<String, String>();
        FileItem theFile = null;
        long size = -1;
        URI uri = null;

        Iterator<FileItem> iter = fileItems.iterator();
        while (iter.hasNext()) {
            FileItem item = (FileItem) iter.next();

            if (item.isFormField()) {
                String name = item.getFieldName();
                String value = null;
                try {
                    value = item.getString("UTF-8").trim();
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }
                LOG.info("Parse head info : " + name + " -- " + value);
                if (name.equals("ObjName")) {
                    try {
                        uri = new URI(value);
                    } catch (URISyntaxException e) {
                        LOG.info("Parse uri info error : " + value);
                        uri = null;
                    }
                } else if (name.equals("ObjSize")) {
                    try {
                        size = Long.parseLong(value);
                    } catch (Exception e) {
                        LOG.error("Parse objSize error : " + value);
                    }
                } else {
                    headMap.put(name, value);
                }
            } else {
                theFile = item;
            }
        }

        if (size == -1 || uri == null || theFile == null || headMap.size() == 0) {
            LOG.error("Parse upload info error : size==-1 || uri == null || theFile == null || headMap.size()==0");
            resp.sendError(
                    500,
                    "Parse upload info error : size==-1 || uri == null || theFile == null || headMap.size()==0");
            return;
        }

        HttpPut put = new HttpPut();
        put.setURI(uri);
        for (Map.Entry<String, String> e : headMap.entrySet()) {
            if ("Filename".equals(e.getKey()))
                continue;
            put.setHeader(e.getKey(), e.getValue());
        }
        put.setEntity(new InputStreamEntity(theFile.getInputStream(), size));
        DefaultHttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(put);
        if (200 != response.getStatusLine().getStatusCode()) {
            LOG.error("Put object error : "
                    + response.getStatusLine().getStatusCode() + " : "
                    + response.getStatusLine().getReasonPhrase());
            resp.sendError(response.getStatusLine().getStatusCode(), response
                    .getStatusLine().getReasonPhrase());
            return;
        }
        LOG.info("Put object OK : " + uri);
        response.setStatusCode(200);
    }

}
