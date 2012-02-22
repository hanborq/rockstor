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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class CompactDirRecordReader extends RecordReader<String, NullWritable> {
    private String taskIdName = null;
    private boolean hasNext = true;
    private static Logger LOG = Logger.getLogger(CompactDirRecordReader.class);

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.taskIdName = ((CompactDirInputSplit) split).getTaskIdName();
        LOG.info("initialize split " + this.taskIdName);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return hasNext;
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
        hasNext = false;
        return taskIdName;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException,
            InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

    @Override
    public void close() throws IOException {

    }

}
