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

import org.apache.hadoop.conf.Configuration;

import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.RockConfiguration;

public class Rock {

    // private static Logger LOG = Logger.getLogger(Rock.class);
    public static final Configuration conf = RockConfiguration.getDefault();
    public static final int ROCK_ID_LEN = 16;
    public static final String HADOOP_DATA_HOME = conf
            .get("rockstor.data.home");

    protected int rockVersion;
    protected String rockID;
    protected byte[] rockMagic;

    protected long ctime = 0;
    protected long garbageBytes = 0;
    protected long deleteBytes = 0;
    protected String writer = "";
    protected boolean retire = false;
    protected long fileSize = 0;
    protected long validSize = 0;

    public Rock() {
    }

    public Rock(byte[] rockMagic) throws IllegalArgumentException {
        setRockMagic(rockMagic);
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public boolean isRetire() {
        return retire;
    }

    public void setRetire(boolean retire) {
        this.retire = retire;
    }

    public void setRockVersion(int rockVersion) {
        this.rockVersion = rockVersion;
    }

    public int getRockVersion() {
        return rockVersion;
    }

    public String getRockID() {
        return rockID;
    }

    public byte[] getRockMagic() {
        return rockMagic;
    }

    public void setRockMagic(byte[] rockMagic) throws IllegalArgumentException {
        if (rockMagic == null || rockMagic.length != ROCK_ID_LEN) {
            throw new IllegalArgumentException("invalid rockMagic");
        }

        this.rockMagic = rockMagic;
        this.rockID = MD5HashUtil.hexStringFromBytes(this.rockMagic);
    }

    public void addGarbageBytes(long newGarbageBytes) {
        if (newGarbageBytes <= 0) {
            return;
        }
        this.garbageBytes += newGarbageBytes;
    }

    /**
     * @param garbageBytes
     *            the garbageBytes to set
     */
    public void setGarbageBytes(long garbageBytes) {
        this.garbageBytes = garbageBytes;
    }

    /**
     * @return the garbageBytes
     */
    public long getGarbageBytes() {
        return garbageBytes;
    }

    @Override
    public String toString() {
        return "Rock [ctime="
                + ctime
                + ", retire="
                + retire
                + ", rockID="
                + rockID
                + ", rockMagic="
                + (rockMagic == null ? "null" : MD5HashUtil
                        .hexStringFromBytes(rockMagic)) + ", rockVersion="
                + rockVersion + ", garbageBytes=" + garbageBytes + ", writer="
                + writer + "]";
    }

    public long getDeleteBytes() {
        return deleteBytes;
    }

    public void setDeleteBytes(long deleteBytes) {
        this.deleteBytes = deleteBytes;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getValidSize() {
        return validSize;
    }

    public void setValidSize(long validSize) {
        this.validSize = validSize;
    }

}
