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

package com.rockstor.clientsample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import com.rockstor.client.AccessControlList;
import com.rockstor.client.AclEntry;
import com.rockstor.client.ListAllMyBucketsResult;
import com.rockstor.client.ListBucketResult;
import com.rockstor.client.RockStor;
import com.rockstor.client.RockStorException;

public class Sample {

    static String address = "10.24.1.10:48080";
    static String username = "testuser";

    private static void hint() {
        System.out.println("Continue? [yes]/[no]");
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));
        try {
            String command = reader.readLine();
            if ("no".equals(command.trim())) {
                System.exit(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void testRockStor() {
        try {

            System.out.println("TEST getService");
            hint();
            RockStor rs = new RockStor(address, username);
            ListAllMyBucketsResult r = rs.getService();
            System.out.println(r);

            System.out.println("TEST createBucket");
            hint();
            rs.createBucket("bucket3");
            r = rs.getService();
            System.out.println(r);

            System.out.println("TEST getBucketAcl");
            hint();
            AccessControlList acl = rs.getBucketAcl("bucket3");
            System.out.println(acl);

            System.out.println("TEST setBucketAcl");
            hint();
            acl = new AccessControlList();
            AclEntry ae = new AclEntry();
            ae.setUser("schubert");
            ae.setAcl("READ");
            ArrayList<AclEntry> list = new ArrayList<AclEntry>();
            list.add(ae);
            acl.setAclEntrys(list);
            rs.setBucketAcl("bucket3", acl);
            acl = rs.getBucketAcl("bucket3");
            System.out.println(acl);

            System.out.println("TEST getBucket");
            hint();
            ListBucketResult r2 = rs.getBucket("bucket3");
            System.out.println(r2);

            System.out.println("TEST putObject");
            hint();
            File testFile = new File("D:/a.txt");
            FileInputStream fis = new FileInputStream(testFile);
            rs.putObject("bucket3", "testobject", null, null, null,
                    (int) testFile.length(), fis);
            fis.close();
            r2 = rs.getBucket("bucket3");
            System.out.println(r2);

            System.out.println("TEST headObject");
            hint();
            Map<String, String> metas = rs.headObject("bucket3", "testobject");
            for (Map.Entry<String, String> e : metas.entrySet()) {
                System.out.println(e.getKey() + " : " + e.getValue());
            }

            System.out.println("TEST setObjectMeta");
            hint();
            metas.clear();
            metas.put("rockstor-meta-key1", "value1");
            metas.put("rockstor-meta-key2", "value2");
            metas.put("rockstor-meta-key3", "value3");
            rs.setObjectMeta("bucket3", "testobject", metas);

            System.out.println("TEST getObjectMeta");
            hint();
            ArrayList m = new ArrayList<String>();
            m.add("rockstor-meta-key1");
            metas = rs.getObjectMeta("bucket3", "testobject", null);
            System.out.println(metas);
            metas = rs.getObjectMeta("bucket3", "testobject", m);
            System.out.println(metas);

            System.out.println("TEST deleteObjectMeta");
            hint();
            rs.deleteObjectMeta("bucket3", "testobject", m);
            metas = rs.getObjectMeta("bucket3", "testobject", null);
            System.out.println(metas);
            rs.deleteObjectMeta("bucket3", "testobject", null);
            metas = rs.getObjectMeta("bucket3", "testobject", null);
            System.out.println(metas);

            System.out.println("TEST getObjectAcl");
            hint();
            acl = rs.getObjectAcl("bucket3", "testobject");
            System.out.println(acl);

            System.out.println("TEST setObjectAcl");
            hint();
            acl = new AccessControlList();
            ae = new AclEntry();
            ae.setUser("schubert");
            ae.setAcl("READ");
            list = new ArrayList<AclEntry>();
            list.add(ae);
            acl.setAclEntrys(list);
            rs.setObjectAcl("bucket3", "testobject", acl);
            acl = rs.getObjectAcl("bucket3", "testobject");
            System.out.println(acl);

            System.out.println("TEST getObject");
            hint();
            InputStream is = rs.getObject("bucket3", "testobject");
            FileOutputStream fos = new FileOutputStream(
                    new File("d:/a.bak.txt"));
            byte[] buf = new byte[4096];
            int len = 0;
            while ((len = is.read(buf)) > 0) {
                fos.write(buf, 0, len);
            }
            fos.close();
            is.close();

            System.out.println("TEST deleteObject");
            hint();
            rs.deleteObject("bucket3", "testobject");

            System.out.println("TEST deleteBucket");
            hint();
            rs.deleteBucket("bucket3");
            r = rs.getService();
            System.out.println(r);

        } catch (RockStorException e) {
            System.out.println(e.getRockStorError());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        testRockStor();
    }
}
