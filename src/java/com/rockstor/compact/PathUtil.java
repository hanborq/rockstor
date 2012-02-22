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

package com.rockstor.compact;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rockstor.util.RockConfiguration;

public class PathUtil {
    private Configuration conf = null;
    private String rootDir = null;
    private String gbMetaDir = null;
    private String taskDir = null;

    public final static String TASK_META_NAME = "meta";
    public final static String TASK_INDEX_NAME = "index";
    public final static String PATH_SEP_STR = "/";

    private PathUtil() {
        conf = RockConfiguration.getDefault();
        rootDir = conf.get("rockstor.compact.dir");

        if (rootDir.endsWith("/")) {
            rootDir = rootDir.substring(0, rootDir.length() - 1);
        }

        taskDir = rootDir + "/task";
        gbMetaDir = rootDir + "/gb_meta";
    }

    private static PathUtil instance = null;

    public static PathUtil getInstance() {
        if (instance == null) {
            instance = new PathUtil();
        }
        return instance;
    }

    public void initRootDir(FileSystem dfs) throws IOException {
        checkAndMakeDir(dfs, rootDir);
        checkAndMakeDir(dfs, gbMetaDir);
        checkAndMakeDir(dfs, taskDir);
    }

    public void createSpecTaskDir(FileSystem dfs, String taskIdName)
            throws IOException {
        checkAndMakeDir(dfs, getSpecTaskDir(taskIdName));
    }

    private void checkAndMakeDir(FileSystem dfs, String dir) throws IOException {
        Path path = new Path(dir);
        if (!dfs.exists(path)) {
            dfs.mkdirs(path);
        }
    }

    public String getRootDir() {
        return rootDir;
    }

    // get meta directory of rock
    public String getGbMetaRootDir() {
        return gbMetaDir;
    }

    // get meta path of rock, file contains deleted chunk info
    public String getGbMetaPath(String rockId) {
        return gbMetaDir + PATH_SEP_STR + rockId;
    }

    // get tasks's root directory
    public String getTaskRootDir() {
        return taskDir;
    }

    // get specified task directory
    public String getSpecTaskDir(String taskIdName) {
        return taskDir + PATH_SEP_STR + taskIdName;
    }

    // get specified task's meta path
    public String getTaskMetaPath(String taskIdName) {
        return taskDir + PATH_SEP_STR + taskIdName + PATH_SEP_STR
                + TASK_META_NAME;
    }

    // get specified task's index path
    public String getTaskIndexPath(String taskIdName) {
        return taskDir + PATH_SEP_STR + taskIdName + PATH_SEP_STR
                + TASK_INDEX_NAME;
    }

    // get specified task's data path
    public String getTaskDataPath(String taskIdName, String rockId) {
        return taskDir + PATH_SEP_STR + taskIdName + PATH_SEP_STR + rockId;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
