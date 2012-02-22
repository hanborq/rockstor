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

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "ListAllMyBucketsResult")
@XmlType(propOrder = { "owner", "buckets" })
public class ListAllMyBucketsResult extends XMLData {

    private String owner;
    private ArrayList<Bucket> buckets;

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
     * @return the buckets
     */
    @XmlElementWrapper(name = "Buckets")
    @XmlElements(@XmlElement(name = "Bucket", type = Bucket.class))
    public ArrayList<Bucket> getBuckets() {
        return buckets;
    }

    /**
     * @param buckets
     *            the buckets to set
     */
    public void setBuckets(ArrayList<Bucket> buckets) {
        this.buckets = buckets;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListAllMyBucketsResult {owner = " + owner + ", Buckets{");
        if (buckets == null) {
            sb.append("NULL");
        } else {
            for (Bucket b : buckets)
                sb.append(b + ", ");
        }
        sb.append("}}");
        return sb.toString();
    }

    public static void main(String[] argv) throws JAXBException, IOException {

        Bucket b1 = new Bucket();
        b1.setName("Name1");
        b1.setCreationDate("CreationDate1");
        Bucket b2 = new Bucket();
        b2.setName("Name2");
        b2.setCreationDate("CreationDate2");
        ArrayList<Bucket> list = new ArrayList<Bucket>();
        list.add(b1);
        list.add(b2);
        ListAllMyBucketsResult r = new ListAllMyBucketsResult();
        r.setOwner("owner");
        r.setBuckets(list);
        FileOutputStream fos = new FileOutputStream("d:/t.txt");

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 1500000; i++) {
            XMLData.serialize(r, fos);
        }
        long t2 = System.currentTimeMillis();

        fos.close();
        System.out.println(t2 - t1);

    }
}
