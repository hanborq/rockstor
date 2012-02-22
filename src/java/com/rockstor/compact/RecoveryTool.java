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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.rockstor.compact.mapreduce.CompactDirInputFormat;
import com.rockstor.compact.recovery.RecoveryMapper;

public class RecoveryTool {
    private String NAME = "Compact_Recovery";
    public static Logger LOG = Logger.getLogger(RecoveryTool.class);

    private Job createSubmittableJob(Configuration conf) throws IOException {
        Job job = new Job(conf, NAME);
        job.setJarByClass(RecoveryTool.class);

        job.setInputFormatClass(CompactDirInputFormat.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);

        job.setMapperClass(RecoveryMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        LOG.info("init job " + NAME + " OK!");
        return job;
    }

    private static RecoveryTool instance = null;

    private RecoveryTool() {
    }

    public static RecoveryTool getInstance() {
        if (instance == null) {
            instance = new RecoveryTool();
        }
        return instance;
    }

    public boolean run() throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        Job job = createSubmittableJob(conf);
        if (job != null) {
            return job.waitForCompletion(true);
        }
        return false;
    }
}
