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

package com.rockstor.core.meta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.webifc.data.AccessControlList;

public class Pebble {
    private static Logger LOG = Logger.getLogger(Pebble.class);

    protected String pebbleID = "";
    protected long size = 0;
    protected long version = 0;

    protected byte[] chunkPrefix = null;
    protected short chunkNum = 0;

    protected byte[] etag = null;

    protected String owner = "";

    protected ACL acl = null; // delay init
    protected String mime = DEFAULT_MINE_TYPE;
    protected HashMap<String, String> meta = null; // delay init
    protected byte[] sign = null;
    protected static String BUCKET_OWNER_READ = "bucket-owner-read"; // only for
                                                                     // object
    protected static String BUCKET_OWNER_FULL_CONTROL = "bucket-owner-full-control"; // only
                                                                                     // for
                                                                                     // object
    protected static String DEFAULT_MINE_TYPE = "application/octet-stream";

    // protected static MimeMap MimeMap.getInstance() = MimeMap.getInstance();

    public byte[] getChunkPrefix() {
        return chunkPrefix;
    }

    public void setChunkPrefix(byte[] chunkPrefix) {
        this.chunkPrefix = chunkPrefix;
    }

    public short getChunkNum() {
        return chunkNum;
    }

    public void setChunkNum(short chunkNum) {
        this.chunkNum = chunkNum;
    }

    public byte[] getEtag() {
        return etag;
    }

    public void setEtag(byte[] etag) {
        this.etag = etag;
    }

    // generator tag
    public void setEtag() throws NoSuchAlgorithmException {
        this.etag = MD5HashUtil.hashCodeBytes(Bytes.toBytes(toString()
                + ", ts=" + System.currentTimeMillis()));
    }

    /**
     * 
     * @return
     */
    public byte[] getSign() {
        if (pebbleID.isEmpty() || version == 0)
            return null;
        if (sign == null) {
            try {
                sign = MD5HashUtil.hashCodeBytes(pebbleID + ":" + version);
            } catch (NoSuchAlgorithmException e) {
                ExceptionLogger.log(LOG, e);
                sign = null;
            }
        }
        return sign;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        if (owner == null || owner.isEmpty()) {
            this.owner = ACL.ALL_USER;
        } else {
            this.owner = owner;
        }
    }

    public void addMeta(String k, String v) {
        if (k == null || k.isEmpty()) {
            return;
        }

        if (meta == null) {
            meta = new HashMap<String, String>();
        }

        if (v == null || v.isEmpty()) {
            meta.remove(k);
        } else {
            meta.put(k, v);
        }
    }

    public String getMeta(String k) {
        if (meta == null) {
            return "";
        }

        String v = meta.get(k);
        if (v == null) {
            return "";
        }

        return v;
    }

    public void setMIME(String type) {
        if (type == null || type.isEmpty()) {
            mime = DEFAULT_MINE_TYPE;
        } else {
            mime = type;
        }
    }

    public void setMIME() {
        mime = DEFAULT_MINE_TYPE;
    }

    public String getMIME() {
        if (mime == null || mime.isEmpty()) {
            mime = DEFAULT_MINE_TYPE;
        }
        return mime;
    }

    public static byte[] Meta2ColumnValue(Pebble pebble) {
        HashMap<String, String> metaMap = pebble.getMeta();

        if (metaMap.isEmpty()) {
            return null;
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bos);

        String curKey = null;
        String curValue = null;
        try {
            for (Map.Entry<String, String> entry : metaMap.entrySet()) {
                curKey = entry.getKey();
                if (curKey == null || curKey.isEmpty()) {
                    continue;
                }

                curValue = entry.getValue();

                if (curValue == null || curValue.isEmpty()) {
                    continue;
                }

                output.writeUTF(curKey);
                output.writeUTF(curValue);
            }
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return bos.toByteArray();
    }

    public static HashMap<String, String> MetaFromColumnValue(byte[] colValue) {
        HashMap<String, String> meta = new HashMap<String, String>();

        if (colValue == null || colValue.length <= 4) {
            return meta;
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(colValue);
        DataInputStream input = new DataInputStream(bis);

        String curKey = null;
        String curValue = null;
        try {
            while (input.available() > 0) {
                curKey = input.readUTF();
                curValue = input.readUTF();
                meta.put(curKey, curValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return meta;
        }

        return meta;
    }

    /**
     * constructor
     */
    public Pebble() {
        this("", 0, 0);
    }

    /**
     * copy constructor
     * 
     * @param pebble
     */
    public Pebble(Pebble pebble) {
        assert (pebble != null);
        this.pebbleID = pebble.pebbleID;
        this.version = pebble.version;
        this.size = pebble.size;
        this.chunkNum = pebble.chunkNum;
        this.chunkPrefix = pebble.chunkPrefix;
        this.acl = pebble.acl;
        this.meta = pebble.meta;
        this.owner = pebble.owner;
        this.mime = pebble.mime;
    }

    public void initACL(String aclStr, String bucketOwner) {
        acl = new ACL();

        // created by Anonymous
        if (owner.equals(ACL.ALL_USER)) {
            acl.append(ACL.ALL_USER, ACL.AclType.READ);
            acl.append(bucketOwner, ACL.AclType.FULL_CONTROL);
        } else {
            if (aclStr == null || aclStr.isEmpty()) {
                return;
            }

            if (aclStr.equals(BUCKET_OWNER_READ)) {
                acl.append(bucketOwner, ACL.AclType.READ);
            } else if (aclStr.equals(BUCKET_OWNER_FULL_CONTROL)) {
                acl.append(bucketOwner, ACL.AclType.FULL_CONTROL);
            } else {
                acl.append(aclStr);
            }
        }
    }

    public void updateACL(String user, String aclStr) {
        if (aclStr == null || aclStr.isEmpty()) {
            return;
        }

        // if created by Anonymous, do nothing
        if (owner.equals(ACL.ALL_USER)) {
            return;
        }

        if (acl == null) {
            acl = new ACL();
        }

        acl.append(user, aclStr);
    }

    public AccessControlList getAclXml() {
        AccessControlList aclXML = null;
        if (acl == null) {
            aclXML = new AccessControlList();
        } else {
            aclXML = acl.toACLXML();
        }

        aclXML.setOwner(owner);

        return aclXML;
    }

    public ACL getAcl() {
        return acl;
    }

    public void setAcl(ACL acl) {
        this.acl = acl;
    }

    public HashMap<String, String> getMeta() {
        return meta;
    }

    public void setMeta(HashMap<String, String> metaIn) {
        if (metaIn == null || metaIn.isEmpty())
            return;
        for (Entry<String, String> entry : metaIn.entrySet()) {
            addMeta(entry.getKey(), entry.getValue());
        }
    }

    public void setPebbleID(String pebbleID) {
        this.pebbleID = pebbleID;
    }

    /**
     * 
     * @param offset
     */
    public Pebble(long offset) {
        this("", 0, 0);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     */
    public Pebble(String pebbleID) {
        this(pebbleID, 0, 0);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     * @param version
     */
    public Pebble(String pebbleID, long version) {
        this(pebbleID, 0, version);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     * @param rockID
     * @param size
     * @param offset
     */
    public Pebble(String pebbleID, byte[] rockID, long size, long offset) {
        this(pebbleID, size, System.currentTimeMillis());
    }

    /**
     * 
     * @param pebbleID
     * @param size
     * @param offset
     * @param version
     */
    public Pebble(String pebbleID, long size, long version) {
        this.pebbleID = pebbleID;

        this.size = size;
        this.version = version;
        this.acl = new ACL();
        this.meta = new HashMap<String, String>();

    }

    /*
     * @param version
     */
    public void setVersion(long version) {
        this.version = version;
    }

    /**
     * set data size
     * 
     * @param size
     */
    public void setSize(long size) {
        this.size = size;
    }

    /**
     * get pebbleID
     * 
     * @return
     */
    public String getPebbleID() {
        return this.pebbleID;
    }

    /**
     * get version
     * 
     * @return
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * get size
     * 
     * @return
     */
    public long getSize() {
        return this.size;
    }

    /**
     * @param user
     * @return
     */
    public boolean canRead(String user) {
        // owner
        if ((!owner.equals(ACL.ALL_USER)) && owner.equals(user)) {
            return true;
        }

        return acl != null && acl.readable(user);
    }

    /**
     * @param user
     * @return
     */
    public boolean canWrite(String user) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param user
     * @return
     */
    public boolean isFullControl(String user) {
        // owner
        if (!owner.equals(ACL.ALL_USER) && owner.equals(user)) {
            return true;
        }

        return acl != null && acl.isFullControl(user);
    }

    public boolean isValid() {
        return pebbleID != null && pebbleID.length() != 0 && version > 0
                && size > 0 && chunkPrefix != null && chunkPrefix.length == 16;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[Pebble: ID=").append(this.pebbleID).append(", ");
        sb.append("version=").append(this.version).append(", ");
        sb.append("owner=").append(this.owner).append(", ");
        sb.append("size=").append(this.size).append(", ");
        sb.append("chunkPrefix=")
                .append(chunkPrefix == null ? "null" : MD5HashUtil
                        .hexStringFromBytes(chunkPrefix)).append(", ");
        sb.append("chunkNum=").append(this.chunkNum).append(", ");
        sb.append("etag=")
                .append((etag == null ? "null" : MD5HashUtil
                        .hexStringFromBytes(etag))).append(", ");
        sb.append("acl=").append(this.acl).append(", ");
        sb.append("meta= [");
        if (meta == null) {
            sb.append("NULL");
        } else {
            sb.append(meta.size());
            sb.append(", ");
            for (Entry<String, String> entry : meta.entrySet()) {
                sb.append(entry.getKey() + "=" + entry.getValue() + ", ");
            }
        }
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Pebble) {
            Pebble pebble = (Pebble) o;
            return this.pebbleID.equals(pebble.pebbleID)
                    && this.version == pebble.version
                    && this.size == pebble.size;
        }
        return false;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Pebble pb = new Pebble("/pic/1.jpg", 64, 1);
        pb.setMIME();
        System.out.println(pb);
        pb.setVersion(System.currentTimeMillis());
        System.out.println(pb);
    }
}
