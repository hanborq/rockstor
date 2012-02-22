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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.rockstor.compact.garbagecollect.GarbageChunkCombine;
import com.rockstor.compact.garbagecollect.GarbageChunkMapper;
import com.rockstor.compact.garbagecollect.GarbageChunkPartition;
import com.rockstor.compact.garbagecollect.GarbageChunkReduce;
import com.rockstor.core.db.GarbageChunkDB;
import com.rockstor.util.RockConfiguration;

public class GenGarbageIndexTool {
    public static Logger LOG = Logger.getLogger(GenGarbageIndexTool.class);
    private String NAME = "Compact_GenGarbageIndex";
    private int batchSize = 1000;

    private Job createSubmittableJob(Configuration conf) throws IOException {
        Job job = new Job(conf, NAME);

        job.setJarByClass(GenGarbageIndexTool.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(GarbageChunkDB.TAB_NAME, scan,
                GarbageChunkMapper.class, ImmutableBytesWritable.class,
                ImmutableBytesWritable.class, job);

        TableMapReduceUtil.setScannerCaching(job, batchSize);
        job.setReducerClass(GarbageChunkReduce.class);
        job.setPartitionerClass(GarbageChunkPartition.class);
        job.setCombinerClass(GarbageChunkCombine.class);

        job.setNumReduceTasks(Compactor.getInstance().getReduceNum());
        job.setOutputFormatClass(NullOutputFormat.class);

        LOG.info("init job " + NAME + " finished!");
        return job;
    }

    private static GenGarbageIndexTool instance = null;

    private GenGarbageIndexTool() {
    }

    public static GenGarbageIndexTool getInstance() {
        if (instance == null) {
            instance = new GenGarbageIndexTool();
        }
        return instance;
    }

    public boolean run() throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create(RockConfiguration
                .getDefault());

        Job job = createSubmittableJob(conf);
        if (job != null) {
            return job.waitForCompletion(true);
        }
        return false;
    }

    public static void main(String[] argv) {
        try {
            GenGarbageIndexTool.getInstance().run();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
