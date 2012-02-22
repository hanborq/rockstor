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

package com.rockstor.compact.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.rockstor.compact.Compactor;
import com.rockstor.compact.PathUtil;

public class CompactDirInputFormat extends InputFormat<String, NullWritable> {
    public static Logger LOG = Logger.getLogger(CompactDirInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        List<InputSplit> inputSplits = new ArrayList<InputSplit>();
        Compactor.getInstance();
        Configuration conf = context.getConfiguration();
        Path rootPath = new Path(PathUtil.getInstance().getTaskRootDir());
        FileSystem dfs = FileSystem.get(conf);

        if (!dfs.exists(rootPath)) {
            return inputSplits;
        }

        FileStatus[] fs = dfs.listStatus(rootPath);
        if (fs == null || fs.length == 0) {
            return inputSplits;
        }

        InputSplit inputSplit = null;
        String taskIdName = null;
        for (FileStatus f : fs) {
            if (!f.isDir()) {
                continue;
            }
            taskIdName = f.getPath().getName();
            LOG.info("add task id name: " + taskIdName);
            inputSplit = new CompactDirInputSplit(taskIdName);
            inputSplits.add(inputSplit);
        }

        return inputSplits;
    }

    @Override
    public RecordReader<String, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {

        CompactDirRecordReader reader = new CompactDirRecordReader();
        // reader.initialize(split, context);
        return reader;
    }

}
