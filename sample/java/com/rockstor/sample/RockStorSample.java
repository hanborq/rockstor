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

package com.rockstor.sample;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class RockStorSample {

    private static final String rockstor = "http://10.24.1.14:8080/rockstor";

    private static final String AUTH = "Authorization";
    private static final String DATE = "Date";
    private static final String CONTENT_LENGTH = "Content-Length";

    private static final String username = "yuanfeng.zhang@qq.com";
    private static final String bucketname = "TestBucket";

    public static String genAuth() {
        return "ROCK0 " + username + ":signature";
    }

    public static String getDate() {
        SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = new Date();
        return sdf.format(date);
    }

    public static void printLine() {
        System.out
                .println("----------------------------------------------------");
    }

    public static void printErrorInfo(HttpURLConnection httpConnection)
            throws JAXBException, IOException {
        InputStream is = httpConnection.getErrorStream();
        JAXBContext deserializeContext = JAXBContext.newInstance(Error.class);
        Unmarshaller unmarshaller = deserializeContext.createUnmarshaller();
        Error error = (Error) unmarshaller.unmarshal(is);
        System.out.println(error);
        is.close();
    }

    public static void getService() throws IOException, JAXBException {
        printLine();
        URL url = new URL(rockstor + "/");
        System.out.println("Exec Get Service : URL - " + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("GET");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 200) {
            System.out.println("Get Service OK.");
            InputStream is = httpConnection.getInputStream();
            JAXBContext deserializeContext = JAXBContext
                    .newInstance(ListAllMyBucketsResult.class);
            Unmarshaller unmarshaller = deserializeContext.createUnmarshaller();
            ListAllMyBucketsResult result = (ListAllMyBucketsResult) unmarshaller
                    .unmarshal(is);
            System.out.println(result);
            is.close();
        } else {
            System.out.println("Get Service Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void putBucket() throws IOException, JAXBException {

        printLine();
        URL url = new URL(rockstor + "/" + bucketname);
        System.out.println("Exec Put Bucket (" + bucketname + ") : URL - "
                + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("PUT");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.setRequestProperty(CONTENT_LENGTH, "0");
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 200) {
            System.out.println("Put Bucket OK.");
        } else {
            System.out.println("Put Bucket Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void delBucket() throws IOException, JAXBException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname);
        System.out.println("Exec Delete Bucket (" + bucketname + ") : URL - "
                + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("DELETE");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 204) {
            System.out.println("Delete Bucket OK.");
        } else {
            System.out.println("Delete Bucket Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void getBucket(String prefix) throws JAXBException,
            IOException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname);
        if (prefix != null)
            url = new URL(url.toString() + "?prefix=" + prefix + "&delimiter=/");
        System.out.println("Exec Get Bucket (" + bucketname + ") : URL - "
                + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("GET");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 200) {
            System.out.println("Get Bucket OK.");
            InputStream is = httpConnection.getInputStream();
            JAXBContext deserializeContext = JAXBContext
                    .newInstance(ListBucketResult.class);
            Unmarshaller unmarshaller = deserializeContext.createUnmarshaller();
            ListBucketResult result = (ListBucketResult) unmarshaller
                    .unmarshal(is);
            System.out.println(result);
            is.close();
        } else {
            System.out.println("Get Bucket Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void putObject(String objName) throws JAXBException,
            IOException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname + "/" + objName);
        System.out.println("Exec Put Object (" + bucketname + "/" + objName
                + ") : URL - " + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("PUT");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.setRequestProperty(CONTENT_LENGTH, "4096");
        httpConnection.connect();
        OutputStream os = httpConnection.getOutputStream();
        byte[] b = new byte[4096];
        os.write(b, 0, b.length);
        os.flush();
        int code = httpConnection.getResponseCode();
        if (code == 200) {
            System.out.println("Put Object OK.");
        } else {
            System.out.println("Put Object Error.");
            printErrorInfo(httpConnection);
        }
        os.close();
        httpConnection.disconnect();
    }

    public static void getObject(String objName) throws JAXBException,
            IOException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname + "/" + objName);
        System.out.println("Exec Get Object (" + bucketname + "/" + objName
                + ") : URL - " + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("GET");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 200) {
            System.out.println("Get Object OK.");
            InputStream is = httpConnection.getInputStream();
            File file = new File(bucketname + "__"
                    + objName.replaceAll("/", "_"));
            FileOutputStream fos = new FileOutputStream(file);
            byte[] b = new byte[1024];
            int len = 0;
            while ((len = is.read(b)) > 0) {
                fos.write(b, 0, len);
            }
            fos.close();
            is.close();
        } else {
            System.out.println("Get Object Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void delObject(String objName) throws JAXBException,
            IOException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname + "/" + objName);
        System.out.println("Exec Delete Object (" + bucketname + "/" + objName
                + ") : URL - " + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("DELETE");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 204) {
            System.out.println("Delete Object OK.");
        } else {
            System.out.println("Delete Object Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();
    }

    public static void HeadObject(String objName) throws JAXBException,
            IOException {
        printLine();
        URL url = new URL(rockstor + "/" + bucketname + "/" + objName);
        System.out.println("Exec Head Object (" + bucketname + "/" + objName
                + ") : URL - " + url);
        HttpURLConnection httpConnection = (HttpURLConnection) url
                .openConnection();
        httpConnection.setRequestMethod("HEAD");
        httpConnection.setDoInput(true);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty(AUTH, genAuth());
        httpConnection.setRequestProperty(DATE, getDate());
        httpConnection.connect();
        int code = httpConnection.getResponseCode();
        if (code == 204) {
            System.out.println("Head Object OK.");
            Map<String, List<String>> map = httpConnection
                    .getRequestProperties();
            for (Map.Entry<String, List<String>> e : map.entrySet()) {
                System.out.print(e.getKey() + ": ");
                for (String v : e.getValue()) {
                    System.out.print(v + "; ");
                }
                System.out.println();
            }
        } else {
            System.out.println("Head Object Error.");
            printErrorInfo(httpConnection);
        }
        httpConnection.disconnect();

    }

    public static void main(String[] args) throws IOException, JAXBException {
        getService();
        putBucket();
        getService();
        putObject("testObject1");
        putObject("dir1/testObject1");
        putObject("dir1/testObject2");
        putObject("dir1/subdir1/testObject1");
        getBucket("dir1/");
        getBucket(null);
        getObject("testObject1");
        delObject("testObject1");
        delObject("dir1/testObject1");
        delObject("dir1/testObject2");
        delObject("dir1/subdir1/testObject1");
        delBucket();
        getService();
    }

}
