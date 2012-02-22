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

package com.rockstor.webifc.data;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "Entry")
@XmlType(propOrder = { "user", "acl" })
public class AclEntry extends XMLData {

    // @XmlEnum(String.class)
    // public enum PERMISSION{READ, WRITE, FULL_CONTROL};

    private String user;
    private String acl;

    // private PERMISSION permission;

    /**
     * @return the user
     */
    @XmlElement(name = "User")
    public String getUser() {
        return user;
    }

    /**
     * @param user
     *            the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the acl
     */
    @XmlElement(name = "Permission")
    public String getAcl() {
        return acl;
    }

    /**
     * @param acl
     *            the acl to set
     */
    public void setAcl(String acl) {
        this.acl = acl;
    }

    // /**
    // * @return the permission
    // */
    // @XmlElement(name="Permission")
    // public PERMISSION getPermission() {
    // return permission;
    // }
    // /**
    // * @param permission the permission to set
    // */
    // public void setPermission(PERMISSION permission) {
    // this.permission = permission;
    // }

    @Override
    public String toString() {
        // return "AclEntry[" + user + ", " + permission + "]";
        return "AclEntry[" + user + ", " + acl + "]";
    }
}
