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

import java.util.ArrayList;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.util.MD5HashUtil;

@XmlRootElement(name = "CompleteMultipartUpload")
@XmlType(propOrder = { "parts" })
public class CompletePartList extends XMLData {
    @XmlRootElement(name = "Part")
    @XmlType(propOrder = { "partNumber", "etag" })
    public static class Part extends XMLData {
        @XmlElement(name = "PartNumber")
        public short getPartNumber() {
            return partNumber;
        }

        public void setPartNumber(short partNumber) {
            this.partNumber = partNumber;
        }

        @XmlElement(name = "ETag")
        public String getEtag() {
            return etag;
        }

        public void setEtag(String etag) {
            this.etag = etag;
        }

        @Override
        public String toString() {
            return "[Part: partNumber=" + partNumber + ", etag=" + etag + "]";
        }

        private short partNumber;
        private String etag;
    }

    private ArrayList<Part> parts;

    /**
     * @param parts
     *            the parts to set
     */
    public void setParts(ArrayList<Part> parts) {
        this.parts = parts;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CompletePartList:{");
        if (parts == null || parts.isEmpty()) {
            sb.append("null");
        } else {
            for (Part p : parts) {
                sb.append(p.toString() + ", ");
            }
        }

        sb.append("}");

        return sb.toString();
    }

    /**
     * @return the parts
     */
    @XmlElements(@XmlElement(name = "Part", type = Part.class))
    public ArrayList<Part> getParts() {
        return parts;
    }

    public static void main(String[] argv) throws JAXBException {
        ArrayList<Part> parts = new ArrayList<Part>();
        for (short i = 0; i < 4; i++) {
            Part p = new Part();
            p.setPartNumber((short) (i + 1));
            p.setEtag(MD5HashUtil.hexStringFromBytes(MultiPartPebble.PartInfo
                    .genRandomEtag()));
            parts.add(p);
        }
        CompletePartList cpl = new CompletePartList();
        cpl.setParts(parts);

        XMLData.init();

        XMLData.serialize(cpl, System.out);

    }
}
