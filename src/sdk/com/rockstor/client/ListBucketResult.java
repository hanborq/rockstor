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
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "ListBucketResult")
@XmlType(propOrder = { "name", "prefix", "marker", "truncated", "maxKeys",
        "contents", "commonPrefixes" })
public class ListBucketResult {

    private String name;
    private String prefix;
    private String marker;
    private boolean truncated;
    private int maxKeys;
    private ArrayList<Contents> contents;
    private ArrayList<CommonPrefixes> commonPrefixes;

    /**
     * @return the name
     */
    @XmlElement(name = "Name")
    public String getName() {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the prefix
     */
    @XmlElement(name = "Prefix")
    public String getPrefix() {
        return prefix;
    }

    /**
     * @param prefix
     *            the prefix to set
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * @return the marker
     */
    @XmlElement(name = "Marker")
    public String getMarker() {
        return marker;
    }

    /**
     * @param marker
     *            the marker to set
     */
    public void setMarker(String marker) {
        this.marker = marker;
    }

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

    /**
     * @return the maxKeys
     */
    @XmlElement(name = "MaxKeys")
    public int getMaxKeys() {
        return maxKeys;
    }

    /**
     * @param maxKeys
     *            the maxKeys to set
     */
    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }

    /**
     * @return the contents
     */
    @XmlElements(@XmlElement(name = "Contents", type = Contents.class))
    public ArrayList<Contents> getContents() {
        return contents;
    }

    /**
     * @param contents
     *            the contents to set
     */
    public void setContents(ArrayList<Contents> contents) {
        this.contents = contents;
    }

    /**
     * @return the commonPrefixes
     */
    @XmlElements(@XmlElement(name = "CommonPrefixes", type = CommonPrefixes.class))
    public ArrayList<CommonPrefixes> getCommonPrefixes() {
        return commonPrefixes;
    }

    /**
     * @param commonPrefixes
     *            the commonPrefixes to set
     */
    public void setCommonPrefixes(ArrayList<CommonPrefixes> commonPrefixes) {
        this.commonPrefixes = commonPrefixes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ListBucketResult {name = " + name + ", prefix = " + prefix
                + ", marker = " + marker + ",truncated = " + truncated
                + ", maxKeys = " + maxKeys + ", Contents{");
        if (contents == null) {
            sb.append("NULL");
        } else {
            for (Contents c : contents)
                sb.append(c + ", ");
        }
        sb.append("}, CommonPrefixes{");
        if (commonPrefixes == null) {
            sb.append("NULL");
        } else {
            for (CommonPrefixes c : commonPrefixes)
                sb.append(c + ", ");
        }
        sb.append("}}");
        return sb.toString();
    }
}
