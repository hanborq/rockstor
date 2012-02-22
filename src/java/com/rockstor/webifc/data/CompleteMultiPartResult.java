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

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "CompleteMultipartUpload")
@XmlType(propOrder = { "bucket", "key", "etag" })
public class CompleteMultiPartResult extends XMLData {
    private String bucket;
    private String key;
    private String etag;

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

    @XmlElement(name = "ETag")
    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    @Override
    public String toString() {
        return "CompleteMultiPartResult { Bucket=" + bucket + ", Key=" + key
                + ", ETag=" + etag + "}";
    }

    /**
     * @param args
     */
    public static void main(String[] argv) throws JAXBException {
        XMLData.init();
        CompleteMultiPartResult r = new CompleteMultiPartResult();
        r.setBucket("testbucket");
        r.setKey("1.jpg");
        r.setEtag("asdfadfasdfdasdfd");
        XMLData.serialize(r, System.out);
    }

}
