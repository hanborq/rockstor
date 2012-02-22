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

import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.util.MD5HashUtil;
import com.rockstor.webifc.data.AccessControlList;

public class Bucket {
    public static Logger LOG = Logger.getLogger(Bucket.class);

    private String id = null;
    private String owner = null;
    private byte[] etag = null;
    private ACL acl = null;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
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

    public Bucket(String id, String owner) {
        this.id = id;
        this.owner = owner;
    }

    public Bucket() {
    }

    public void initACL(String aclStr) {
        acl = new ACL();
        if (aclStr == null || aclStr.isEmpty()) {
            return;
        }
        acl.append(aclStr);
    }

    public void updateACL(String user, String aclStr) {
        if (user == null || user.isEmpty() || aclStr == null
                || aclStr.isEmpty()) {
            return;
        }

        if (acl == null) {
            acl = new ACL();
        }

        acl.append(user, aclStr);
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
        // owner
        if ((!owner.equals(ACL.ALL_USER)) && owner.equals(user)) {
            return true;
        }

        return acl != null && acl.writable(user);
    }

    /**
     * @param user
     * @return
     */
    public boolean isFullControl(String user) {
        // owner
        if ((!owner.equals(ACL.ALL_USER)) && owner.equals(user)) {
            return true;
        }

        return acl != null && acl.isFullControl(user);
    }

    @Override
    public String toString() {
        return "Bucket [acl="
                + acl
                + ", etag="
                + (etag == null ? "null" : MD5HashUtil.hexStringFromBytes(etag))
                + ", id=" + id + ", owner=" + owner + "]";
    }
}
