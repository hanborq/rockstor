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

package com.rockstor.compact.recovery;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.compact.Compactor;
import com.rockstor.compact.PathUtil;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.io.RockAccessor;
import com.rockstor.util.MD5HashUtil;

public class CompactRecovery {
    private static Logger LOG = Logger.getLogger(CompactRecovery.class);
    private Path dirPath = null;
    private FileSystem dfs = null;
    private String taskIdName = null;
    private Compactor compactor = null;

    public CompactRecovery(String taskIdName) {
        compactor = Compactor.getInstance();
        this.taskIdName = taskIdName;
        this.dirPath = new Path(PathUtil.getInstance().getSpecTaskDir(
                this.taskIdName));
        this.dfs = RockAccessor.getFileSystem();
    }

    public void recovery() throws IOException, NoSuchAlgorithmException {
        if (!dfs.exists(dirPath)) {
            LOG.info("directory is not exists" + dirPath + "!");
            return;
        }

        if (dfs.isFile(dirPath)) {
            LOG.error("input path (" + dirPath + ") should be a directory!");
            throw new IOException("Input Error");
        }

        FileStatus[] files = dfs.listStatus(dirPath);

        if (files == null || files.length == 0) {
            LOG.info("empty directory " + dirPath + "!");
            return;
        }

        Path dataPath = null;
        Path metaPath = null;
        Path indexPath = null;

        Path curPath = null;
        String curName = null;
        String rockIdStr = null;
        byte[] rockId = null;

        for (FileStatus f : files) {
            curPath = f.getPath();

            curName = curPath.getName();

            if (curName.equals(PathUtil.TASK_INDEX_NAME)) {
                indexPath = curPath;
            } else if (curName.equals(PathUtil.TASK_META_NAME)) {
                metaPath = curPath;
            } else {
                dataPath = curPath;
                rockIdStr = curName;
                rockId = MD5HashUtil.bytesFromHexString(rockIdStr);
            }
        }

        /*
         * in compact subdir, files co-exists in such situation: 1. null not
         * started, or finished 2. only meta not started 3. meta, data, not
         * started, we created data before index. 4. meta, data, index started,
         * but failed 5. meta, index data had been compacted, but chunk index
         * had not been synchronized 6. only index data had been compacted, and
         * invalid chunks had been removed from chunk db need sync left chunk
         * index 7. null total finished
         */
        do {
            // compact failed, remove failed rock from rock db, situation 3 or 4
            if (dataPath != null) {
                RockDB.remove(rockId);
                break;
            }

            if (metaPath != null && indexPath == null) { // situation 2
                break;
            }

            if (metaPath != null) { // situation 5
                // remove invalid chunks
                compactor.removeInvalidChunks(taskIdName);
            }

            if (indexPath != null) { // situation 5 or 6
                // sync left chunks
                compactor.syncLeftChunks(taskIdName);
            }
        } while (false);

        // remove current path
        dfs.delete(dirPath, true);
    }

    /**
     * @return the dirPath
     */
    public Path getDirPath() {
        return dirPath;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

    }
}
