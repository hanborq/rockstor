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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.TreeMap;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.util.DateUtil;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.UploadIdGen;

@XmlRootElement(name = "ListPartsResult")
@XmlType(propOrder = { "bucket", "key", "uploadId", "owner",
        "partNumberMarker", "maxParts", "truncated", "part" })
public class ListPartsResult extends XMLData {
    @XmlRootElement(name = "Part")
    @XmlType(propOrder = { "partNumber", "lastModified", "etag", "size" })
    public static class Part extends XMLData {
        private short partNumber;
        private String lastModified;
        private String etag;
        private long size;

        public Part() {
            super();
        }

        public Part(MultiPartPebble.PartInfo partInfo) {
            super();
            setPartInfo(partInfo);
        }

        @XmlElement(name = "PartNumber")
        public short getPartNumber() {
            return partNumber;
        }

        @XmlElement(name = "LastModified")
        public String getLastModified() {
            return lastModified;
        }

        @XmlElement(name = "ETag")
        public String getEtag() {
            return etag;
        }

        @XmlElement(name = "Size")
        public long getSize() {
            return size;
        }

        /**
         * @param partInfo
         *            the partInfo to set
         */
        public void setPartInfo(MultiPartPebble.PartInfo partInfo) {
            this.size = partInfo.size;
            this.partNumber = partInfo.partId;
            this.etag = MD5HashUtil.hexStringFromBytes(partInfo.etag);
            this.lastModified = DateUtil.long2Str(partInfo.timestamp);
        }

        @Override
        public String toString() {
            return "Part[" + this.getPartNumber() + ", "
                    + this.getLastModified() + ", " + this.getEtag() + ", "
                    + this.getSize() + "]";
        }
    }

    public ListPartsResult() {
        super();
    }

    public static final short DEFAULT_MAX_PARTS = 1000;
    public static final short DEFAULT_PART_MARKER = 0;

    public ListPartsResult(MultiPartPebble pebble, String bucket, String object) {
        this(pebble, bucket, object, DEFAULT_MAX_PARTS, DEFAULT_PART_MARKER);
    }

    public ListPartsResult(MultiPartPebble pebble, String bucket,
            String object, short maxParts) {
        this(pebble, bucket, object, maxParts, DEFAULT_PART_MARKER);
    }

    public ListPartsResult(MultiPartPebble pebble, String bucket,
            String object, short maxParts, short partNumberMarker) {
        super();

        this.bucket = bucket;
        this.key = object;
        this.maxParts = maxParts;
        this.partNumberMarker = partNumberMarker;
        try {
            this.uploadId = UploadIdGen.ver2UploadId(pebble.getPebbleID(),
                    pebble.getVersion());
        } catch (NoSuchAlgorithmException e) {
            this.uploadId = "null";
        }
        this.owner = pebble.getOwner();
        this.truncated = false;
        if (maxParts <= 0) {
            return;
        }

        TreeMap<Short, MultiPartPebble.PartInfo> partInfos = pebble.getParts();

        if (partInfos == null || partInfos.isEmpty()) {
            return;
        }

        short partNum = 0;
        short index = 0;
        for (MultiPartPebble.PartInfo partInfo : partInfos.values()) {
            ++index;
            if (partInfo.partId > partNumberMarker) {
                if (this.part == null) {
                    this.part = new ArrayList<Part>();
                }
                this.part.add(new Part(partInfo));
                if ((++partNum) >= maxParts) {
                    break;
                }
            }
        }

        this.truncated = (index != partInfos.size());
    }

    @XmlElement(name = "Bucket")
    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    @XmlElement(name = "Key")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @XmlElement(name = "UploadId")
    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    @XmlElement(name = "Owner")
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @XmlElement(name = "PartNumberMarker")
    public short getPartNumberMarker() {
        return partNumberMarker;
    }

    public void setPartNumberMarker(short partNumberMarker) {
        this.partNumberMarker = partNumberMarker;
    }

    @XmlElement(name = "MaxParts")
    public short getMaxParts() {
        return maxParts;
    }

    public void setMaxParts(short maxParts) {
        this.maxParts = maxParts;
    }

    @XmlElements(@XmlElement(name = "Part", type = Part.class))
    public ArrayList<Part> getPart() {
        return part;
    }

    public void setParts(ArrayList<Part> part) {
        this.part = part;
    }

    private String bucket;
    private String key;
    private String uploadId;
    private String owner;
    private short partNumberMarker;
    private short maxParts;
    private boolean truncated;

    private ArrayList<Part> part;

    /**
     * @return the truncated
     */
    @XmlElement(name = "IsTruncated")
    public boolean isTruncated() {
        return truncated;
    }

    /**
     * @param truncated
     *            the truncated to set
     */
    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListPartsResult {Bucket = " + bucket + ", Key = " + key
                + ", uploadId = " + uploadId + ",truncated = " + truncated
                + ", maxParts = " + maxParts + ", partNumberMarker="
                + partNumberMarker + ", Parts{");
        if (part == null) {
            sb.append("NULL");
        } else {
            for (Part c : part)
                sb.append(c + ", ");
        }
        sb.append("}}");
        return sb.toString();
    }

    public static void main(String argv[]) {
        XMLData.init();
        MultiPartPebble pebble = new MultiPartPebble();
        pebble.setPebbleID("/testbucket/pebble001.jpg");
        pebble.setOwner("terry");
        MultiPartPebble.PartInfo partInfo = null;
        long ts = System.currentTimeMillis();
        for (int i = 1; i < 6; i++) {
            partInfo = new MultiPartPebble.PartInfo((short) i, 10 * i);
            partInfo.setTimestamp(ts + 100);
            pebble.addPart(partInfo);
        }

        ListPartsResult lpr = new ListPartsResult(pebble, "testbucket",
                "pebble001.jpg");
        System.out.println(lpr);
        try {
            XMLData.serialize(lpr, System.out);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        System.out.println();
        System.out.println();
        lpr = new ListPartsResult(pebble, "testbucket", "pebble001.jpg",
                (short) 2, (short) 1);
        System.out.println(lpr);
        try {
            XMLData.serialize(lpr, System.out);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        System.out.println();

    }
}
