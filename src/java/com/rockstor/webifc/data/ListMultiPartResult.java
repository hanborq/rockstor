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

import com.rockstor.util.DateUtil;

@XmlRootElement(name = "ListMultipartUploadsResult")
@XmlType(propOrder = { "bucket", "keyMarker", "uploadIdMarker",
        "nextKeyMarker", "nextUploadIdMarker", "delimiter", "prefix",
        "maxUploads", "truncated", "uploads", "commonPrefixes" })
public class ListMultiPartResult extends XMLData {
    @XmlRootElement(name = "Upload")
    @XmlType(propOrder = { "key", "uploadId", "initiator" })
    public static class Upload extends XMLData {
        private String key;
        private String uploadId;
        private String initiator;

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

        @XmlElement(name = "Initiator")
        public String getInitiator() {
            return initiator;
        }

        public void setInitiator(String initiator) {
            this.initiator = initiator;
        }

        @Override
        public String toString() {
            return "[Upload: Key=" + key + ", UploadId=" + uploadId
                    + ", Initiator=" + initiator + "]";
        }
    }

    private String bucket;
    private String keyMarker;
    private String uploadIdMarker;
    private String nextKeyMarker;
    private String nextUploadIdMarker;
    private String delimiter;
    private String prefix;
    private int maxUploads;
    private boolean truncated;
    private ArrayList<Upload> uploads;
    private ArrayList<CommonPrefixes> commonPrefixes;

    @XmlElement(name = "Bucket")
    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    @XmlElement(name = "KeyMarker")
    public String getKeyMarker() {
        return keyMarker;
    }

    public void setKeyMarker(String keyMarker) {
        this.keyMarker = keyMarker;
    }

    @XmlElement(name = "UploadIdMarker")
    public String getUploadIdMarker() {
        return uploadIdMarker;
    }

    public void setUploadIdMarker(String uploadIdMarker) {
        this.uploadIdMarker = uploadIdMarker;
    }

    @XmlElement(name = "NextKeyMarker")
    public String getNextKeyMarker() {
        return nextKeyMarker;
    }

    public void setNextKeyMarker(String nextKeyMarker) {
        this.nextKeyMarker = nextKeyMarker;
    }

    @XmlElement(name = "NextUploadIdMarker")
    public String getNextUploadIdMarker() {
        return nextUploadIdMarker;
    }

    public void setNextUploadIdMarker(String nextUploadIdMarker) {
        this.nextUploadIdMarker = nextUploadIdMarker;
    }

    @XmlElement(name = "MaxUploads")
    public int getMaxUploads() {
        return maxUploads;
    }

    public void setMaxUploads(int maxUploads) {
        this.maxUploads = maxUploads;
    }

    @XmlElement(name = "IsTruncated")
    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    @XmlElements(@XmlElement(name = "Upload", type = Upload.class))
    public ArrayList<Upload> getUploads() {
        return uploads;
    }

    public void setUploads(ArrayList<Upload> uploads) {
        this.uploads = uploads;
    }

    /**
     * @param commonPrefixes
     *            the commonPrefixes to set
     */
    @XmlElements(@XmlElement(name = "CommonPrefixes", type = CommonPrefixes.class))
    public ArrayList<CommonPrefixes> getCommonPrefixes() {
        return this.commonPrefixes;
    }

    /**
     * @param commonPrefixes
     *            the commonPrefixes to set
     */
    public void setCommonPrefixes(ArrayList<CommonPrefixes> commonPrefixes) {
        this.commonPrefixes = commonPrefixes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListMultiPart {");
        sb.append("Bucket=").append(bucket);
        sb.append(", KeyMarker=").append(keyMarker);
        sb.append(", UploadIdMarker=").append(uploadIdMarker);
        sb.append(", NextKeyMarker=").append(nextKeyMarker);
        sb.append(", NextUploadIdMarker=").append(nextUploadIdMarker);
        sb.append(", MaxUploads=").append(maxUploads);
        sb.append(", IsTruncated=").append(truncated);
        sb.append(", Uploads={");

        if (uploads == null || uploads.isEmpty()) {
            sb.append("null");
        } else {
            for (Upload upload : uploads) {
                sb.append(upload.toString()).append(", ");
            }
        }

        sb.append("}}");

        return sb.toString();
    }

    public static void main(String[] args) throws JAXBException {
        XMLData.init();

        ListMultiPartResult lmp = new ListMultiPartResult();
        lmp.setBucket("testbucket");
        lmp.setKeyMarker("***keyMarker***");
        lmp.setUploadIdMarker("***uploadIdMarker***");
        lmp.setMaxUploads(10);
        lmp.setNextKeyMarker("***nextKeyMarker***");
        lmp.setNextUploadIdMarker("*** nextUploadIdMarker***");
        lmp.setTruncated(false);

        ArrayList<Upload> uploads = new ArrayList<Upload>();
        ArrayList<CommonPrefixes> cps = new ArrayList<CommonPrefixes>();

        lmp.setCommonPrefixes(cps);
        lmp.setUploads(uploads);
        CommonPrefixes cp = null;
        for (int i = 0; i < 3; i++) {
            cp = new CommonPrefixes();
            cp.setPrefix("prefix_" + i);
            cps.add(cp);
        }

        Upload upload = null;
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            upload = new Upload();
            upload.setInitiator(DateUtil.long2Str(ts + (i * 10)));
            upload.setKey("key_" + i);
            upload.setUploadId("uploadId_" + i);
            uploads.add(upload);
        }

        System.out.println(lmp.toString());
        System.out.println();
        XMLData.serialize(lmp, System.out);
    }

    @XmlElement(name = "Delimiter")
    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @XmlElement(name = "Prefix")
    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
