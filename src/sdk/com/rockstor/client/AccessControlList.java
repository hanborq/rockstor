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

package com.rockstor.client;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "AccessControlList")
@XmlType(propOrder = { "owner", "aclEntrys" })
public class AccessControlList {

    private String owner;
    private ArrayList<AclEntry> aclEntrys;

    /**
     * @return the owner
     */
    @XmlElement(name = "Owner")
    public String getOwner() {
        return owner;
    }

    /**
     * @param owner
     *            the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * @return the aclEntrys
     */
    @XmlElementWrapper(name = "Entrys")
    @XmlElements(@XmlElement(name = "Entry", type = AclEntry.class))
    public ArrayList<AclEntry> getAclEntrys() {
        return aclEntrys;
    }

    /**
     * @param aclEntrys
     *            the aclEntrys to set
     */
    public void setAclEntrys(ArrayList<AclEntry> aclEntrys) {
        this.aclEntrys = aclEntrys;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AccessControlList { owner = " + owner + ", Entrys{");
        if (aclEntrys == null)
            sb.append("NULL");
        else {
            for (AclEntry entry : aclEntrys)
                sb.append(entry + ", ");
        }
        sb.append("}}");
        return sb.toString();
    }
}
