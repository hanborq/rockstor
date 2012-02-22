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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Pair;

import com.rockstor.webifc.data.AccessControlList;
import com.rockstor.webifc.data.AclEntry;

public class ACL {
    private HashMap<String, AclType> aclMap = null;

    public static enum AclType {
        READ, WRITE, FULL_CONTROL,
    }

    public static String PRIVATE = "private";
    public static String PUBLIC_READ = "public-read";
    public static String PUBLIC_READ_WRITE = "public-read-write";
    public static String AUTHENTICATED_READ = "authenticated-read";

    public static String ALL_USER = "AllUsers";
    public static String ALL_AUTHENTICATED_USERS = "AllAuthenticatedUsers";

    private static HashMap<String, Pair<String, AclType>> specACLMap = new HashMap<String, Pair<String, AclType>>();

    static {
        specACLMap.put(PRIVATE, null);
        specACLMap.put(PUBLIC_READ, new Pair<String, AclType>(ALL_USER,
                AclType.READ));
        specACLMap.put(PUBLIC_READ_WRITE, new Pair<String, AclType>(ALL_USER,
                AclType.WRITE));
        specACLMap.put(AUTHENTICATED_READ, new Pair<String, AclType>(
                ALL_AUTHENTICATED_USERS, AclType.READ));
    }

    private static AclType[] TYPES = AclType.values();

    private static AclType getType(int ord) {
        if (ord < 0 || ord >= TYPES.length) {
            throw new IllegalArgumentException("AclType Index[" + ord
                    + "] out of range!");
        }
        return TYPES[ord];
    }

    public ACL() {
        aclMap = new HashMap<String, AclType>();
    }

    public HashMap<String, AclType> getEntryMap() {
        return aclMap;
    }

    public static byte[] toColumnValue(ACL acl) {
        HashMap<String, AclType> aclMap = acl.getEntryMap();

        if (aclMap.isEmpty()) {
            return null;
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bos);

        String curUser = null;
        try {
            for (Map.Entry<String, AclType> entry : aclMap.entrySet()) {
                curUser = entry.getKey();
                if (curUser == null || curUser.isEmpty()) {
                    continue;
                }

                output.writeUTF(curUser);
                output.writeInt(entry.getValue().ordinal());
            }
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return bos.toByteArray();
    }

    public static ACL fromColumnValue(byte[] colValue) {
        ACL acl = new ACL();

        if (colValue == null || colValue.length <= 4) {
            return acl;
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(colValue);
        DataInputStream input = new DataInputStream(bis);

        String curUser = null;
        int curAclType = 0;
        try {
            while (input.available() > 0) {
                curUser = input.readUTF();
                curAclType = input.readInt();
                acl.append(curUser, curAclType);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return acl;
        }

        return acl;
    }

    public void setEntryMap(HashMap<String, AclType> aclMap) {
        if (aclMap == null) {
            this.aclMap = new HashMap<String, AclType>();
        } else {
            this.aclMap = aclMap;
        }
    }

    public void append(String acl) throws IllegalArgumentException {
        if (!specACLMap.containsKey(acl))
            throw new IllegalArgumentException();
        Pair<String, AclType> p = specACLMap.get(acl);
        if (p != null) {
            append(p.getFirst(), p.getSecond());
        }
    }

    public void append(String user, int ord) throws IllegalArgumentException {
        this.append(user, getType(ord));
    }

    public void append(String user, AclType t) {
        aclMap.put(user, t);
    }

    public void append(Map<String, String> acls)
            throws IllegalArgumentException {
        for (Entry<String, String> e : acls.entrySet()) {
            this.append(e.getKey(), e.getValue());
        }
    }

    public void append(AccessControlList acl) throws IllegalArgumentException {
        ArrayList<AclEntry> list = acl.getAclEntrys();
        for (AclEntry e : list) {
            this.append(e.getUser(), e.getAcl());
        }
    }

    public void append(String user, String acl) throws IllegalArgumentException {
        aclMap.put(user, AclType.valueOf(acl));
    }

    public void remove(String user) {
        aclMap.remove(user);
    }

    public void clean() {
        aclMap.clear();
    }

    public boolean readable(String user) {
        if (aclMap.containsKey(ALL_USER))
            return true;

        if (user == null || user.isEmpty()) {
            return false;
        }

        return aclMap.containsKey(user)
                || aclMap.containsKey(ALL_AUTHENTICATED_USERS);
    }

    public boolean writable(String user) {
        AclType acl = aclMap.get(ALL_USER);

        if (acl != null && acl != AclType.READ) {
            return true;
        }

        if (user == null || user.isEmpty()) {
            return false;
        }

        acl = aclMap.get(user);

        if (acl != null && acl != AclType.READ) {
            return true;
        }

        acl = aclMap.get(ALL_AUTHENTICATED_USERS);

        return acl != null && acl != AclType.READ;
    }

    public boolean isFullControl(String user) {
        AclType acl = aclMap.get(ALL_USER);

        if (acl != null && acl == AclType.FULL_CONTROL) {
            return true;
        }

        if (user == null || user.isEmpty()) {
            return false;
        }

        acl = aclMap.get(user);

        if (acl != null && acl == AclType.FULL_CONTROL) {
            return true;
        }

        acl = aclMap.get(ALL_AUTHENTICATED_USERS);

        return acl != null && acl == AclType.FULL_CONTROL;
    }

    public AccessControlList toACLXML() {
        AccessControlList acl = new AccessControlList();
        ArrayList<AclEntry> list = new ArrayList<AclEntry>();
        AclEntry aclEntry = null;
        for (Entry<String, AclType> entry : aclMap.entrySet()) {
            aclEntry = new AclEntry();
            aclEntry.setUser(entry.getKey());
            aclEntry.setAcl(entry.getValue().toString());
            list.add(aclEntry);
        }
        acl.setAclEntrys(list);
        return acl;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("ACL [");
        if (aclMap != null)
            for (Entry<String, AclType> e : aclMap.entrySet()) {
                sb.append(e.getKey() + "=" + e.getValue() + ", ");
            }
        sb.append("]");
        return sb.toString();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ACL acl = new ACL();
        acl.append("terry", AclType.READ);
        acl.append("white", AclType.WRITE);
        acl.append("someone", AclType.FULL_CONTROL);
        try {
            acl.append("someone", "READ");
            acl.append("a", "asf");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println(acl);

        System.out.println(AclType.READ.name());
        System.out.println(AclType.READ.ordinal());

        byte[] bytes = ACL.toColumnValue(acl);
        ACL a = ACL.fromColumnValue(bytes);
        System.out.println(a);
    }

}
