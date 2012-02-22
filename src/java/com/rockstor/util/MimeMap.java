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

package com.rockstor.util;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MimeMap {
    private static Logger LOG = Logger.getLogger(MimeMap.class);

    private final static String confFileName = "mime-map.xml";
    private final static String TAG_ROOT = "default-mime-map";
    private final static String TAG_ENTRY = "mime-mapping";
    private final static String TAG_NAME = "extension";
    private final static String TAG_VALUE = "mime-type";

    private static MimeMap instance;
    private HashMap<String, String> nameValueMap;
    private static final String default_mime_type = "application/octet-stream";

    public static MimeMap getInstance() {
        if (instance == null)
            instance = new MimeMap();
        return instance;
    }

    private MimeMap() {
        LOG.info("Begin init MimeMap...");

        URL url = MimeMap.class.getClassLoader().getResource(confFileName);
        if (url == null) {
            LOG.error(confFileName + " : Load Error: not exist.");
            System.exit(-1);
        }

        File file = new File(url.getFile());

        if (file.exists()) {
            LOG.info(confFileName + " : " + file.getAbsolutePath());
        } else {
            LOG.error(confFileName + " : Load Error: not exist.");
            System.exit(-1);
        }

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(file);

            NodeList rootNodes = document.getChildNodes();
            Node rootNode = null;

            int propNum = rootNodes.getLength();
            for (int i = 0; i < propNum; i++) {
                Node node = rootNodes.item(i);
                if (node instanceof Element
                        && node.getNodeName().equals(TAG_ROOT)) {
                    rootNode = node;
                    break;
                }
            }

            if (rootNode == null) {
                LOG.error("Cannnot find ROOT_TAG : " + TAG_ROOT);
                System.exit(-1);
            }

            NodeList properties = rootNode.getChildNodes();
            nameValueMap = new HashMap<String, String>();
            propNum = properties.getLength();
            for (int i = 0; i < propNum; i++) {
                Node node = properties.item(i);
                if (node instanceof Element
                        && node.getNodeName().equals(TAG_ENTRY)) {

                    NodeList nameValeList = node.getChildNodes();
                    int num = nameValeList.getLength();
                    Node nameNode = null;
                    Node valueNode = null;

                    for (int j = 0; j < num; j++) {
                        Node n = nameValeList.item(j);
                        if (n instanceof Element
                                && n.getNodeName().equals(TAG_NAME))
                            nameNode = n;
                        else if (n instanceof Element
                                && n.getNodeName().equals(TAG_VALUE))
                            valueNode = n;
                    }

                    if (nameNode == null || valueNode == null)
                        continue;

                    String name = nameNode.getTextContent().trim();
                    String value = valueNode.getTextContent().trim();
                    if (name.equals("") || value.equals(""))
                        continue;

                    LOG.debug("Get propertity : " + name + " - " + value);
                    nameValueMap.put(name, value);
                }
            }

        } catch (Exception e) {
            LOG.info("Parse " + confFileName + " ERROR : " + e);
            System.exit(-1);
        }
    }

    public String getMimeByName(String fName) {
        int pos = 0;
        String extName = null;
        pos = fName.lastIndexOf(".");
        if (pos != -1) {
            extName = fName.substring(pos + 1);
        }
        return getMime(extName);
    }

    public String getMime(String key, String defaultValue) {

        if (nameValueMap.containsKey(key)) {
            String valueString = nameValueMap.get(key);
            return valueString;
        } else {
            return defaultValue;
        }
    }

    public String getMime(String key) {
        return getMime(key, default_mime_type);
    }

    public HashMap<String, String> getDefaultMap() {
        return nameValueMap;
    }

    public static void checkMIME(MimeMap mime, File f) {
        int pos = 0;
        String fName = f.getName();
        String extName = null;
        if (f.isFile()) {
            pos = fName.lastIndexOf(".");
            if (pos != -1) {
                extName = fName.substring(pos + 1);
            }

            System.out.println(fName + " : " + mime.getMime(extName));
            return;
        } else if (f.isDirectory()) {
            for (File subf : f.listFiles()) {
                checkMIME(mime, subf);
            }
        }
    }

    public static void main(String[] argv) {
        // RockConf.getConf();
        MimeMap mime = MimeMap.getInstance();
        HashMap<String, String> m = mime.getDefaultMap();
        for (Entry<String, String> entry : m.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
