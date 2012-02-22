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
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

/**
 * the class is used to generate md5 of bytes
 * 
 * @author Cestbon
 * 
 */
public class MD5HashUtil {
    // private MessageDigest md = null;
    private static MD5HashUtil md5 = null;
    private static final char[] hexChars = { '0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    private static HashMap<Character, Byte> charMap = new HashMap<Character, Byte>();
    static {
        charMap.put('0', (byte) 0x00);
        charMap.put('1', (byte) 0x01);
        charMap.put('2', (byte) 0x02);
        charMap.put('3', (byte) 0x03);
        charMap.put('4', (byte) 0x04);
        charMap.put('5', (byte) 0x05);
        charMap.put('6', (byte) 0x06);
        charMap.put('7', (byte) 0x07);
        charMap.put('8', (byte) 0x08);
        charMap.put('9', (byte) 0x09);
        charMap.put('a', (byte) 0x0a);
        charMap.put('b', (byte) 0x0b);
        charMap.put('c', (byte) 0x0c);
        charMap.put('d', (byte) 0x0d);
        charMap.put('e', (byte) 0x0e);
        charMap.put('f', (byte) 0x0f);
    }

    /**
     * Constructor is private so you must use the getInstance method
     */
    private MD5HashUtil() throws NoSuchAlgorithmException {
        // md = MessageDigest.getInstance("MD5");
    }

    /**
     * This returns the singleton instance
     */
    private static MD5HashUtil getInstance() throws NoSuchAlgorithmException {
        if (md5 == null) {
            md5 = new MD5HashUtil();
        }
        return (md5);
    }

    public static byte[] hashCodeBytes(File file)
            throws NoSuchAlgorithmException, IOException {
        return getInstance().calculateHash(file);
    }

    public static byte[] hashCodeBytes(String dataToHash)
            throws NoSuchAlgorithmException {
        return getInstance().calculateHash(dataToHash.getBytes());
    }

    public static byte[] hashCodeBytes(byte[] dataToHash)
            throws NoSuchAlgorithmException {
        return getInstance().calculateHash(dataToHash);
    }

    public static byte[] hashCodeBytes(byte[] dataToHash, int begin, int length)
            throws NoSuchAlgorithmException {
        return getInstance().calculateHash(dataToHash, begin, length);
    }

    public static String hashCode(File file) throws NoSuchAlgorithmException,
            IOException {
        return getInstance().hashData(file);
    }

    public static String hashCode(String dataToHash)
            throws NoSuchAlgorithmException {
        return getInstance().hashData(dataToHash.getBytes());
    }

    public static String hashCode(byte[] dataToHash)
            throws NoSuchAlgorithmException {
        return getInstance().hashData(dataToHash);
    }

    public static String hashCode(byte[] dataToHash, int begin, int length)
            throws NoSuchAlgorithmException {
        return getInstance().hashData(dataToHash, begin, length);
    }

    private String hashData(File file) throws IOException {
        return hexStringFromBytes(calculateHash(file));
    }

    private String hashData(byte[] dataToHash) {
        return hexStringFromBytes((calculateHash(dataToHash)));
    }

    private String hashData(byte[] dataToHash, int begin, int length) {
        return hexStringFromBytes((calculateHash(dataToHash, begin, length)));
    }

    private byte[] calculateHash(File file) throws IOException {

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        FileInputStream input = new FileInputStream(file);

        byte[] b = new byte[4096];
        int len = 0;
        while ((len = input.read(b)) > 0) {
            md.update(b, 0, len);
        }
        input.close();
        return (md.digest());
    }

    private byte[] calculateHash(byte[] dataToHash) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        md.update(dataToHash, 0, dataToHash.length);
        return (md.digest());
    }

    private byte[] calculateHash(byte[] dataToHash, int begin, int end) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        md.update(dataToHash, begin, end);
        return (md.digest());
    }

    public static String hexStringFromBytes(byte[] b) {
        String hex = "";
        int msb;
        int lsb = 0;
        int i;
        // MSB maps to idx 0
        for (i = 0; i < b.length; i++) {
            msb = (b[i] & 0x000000FF) / 16;
            lsb = (b[i] & 0x000000FF) % 16;
            hex = hex + hexChars[msb] + hexChars[lsb];
        }
        return (hex);
    }

    public static byte[] bytesFromHexString(String string) {
        byte[] ret = new byte[16];
        for (int i = 0; i < 32; i += 2) {
            ret[i / 2] = (byte) ((charMap.get(string.charAt(i)) << 4) | (charMap
                    .get(string.charAt(i + 1))));
        }
        return ret;
    }

    public static void main(String args[]) throws NoSuchAlgorithmException,
            IOException {
        String string = "rss123";
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            hashCode(string);
        }
        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }
}
