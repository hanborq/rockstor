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

package com.rockstor.compact.garbagecollect;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.rockstor.compact.Compactor;
import com.rockstor.compact.PathUtil;
import com.rockstor.compact.RockIndexWriter;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.meta.Chunk;
import com.rockstor.util.MD5HashUtil;

public class GarbageChunkReduce
        extends
        Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    public static Logger LOG = Logger.getLogger(GarbageChunkReduce.class);
    private Chunk chunk = null;
    private boolean notFirstRock = false;
    private byte[] lastRockId = Bytes.add(Bytes.toBytes(0L), Bytes.toBytes(0L));
    private long lastRockDeleteSize = 0;
    private RockIndexWriter rockIndexWriter = null;

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Compactor.getInstance();
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    @Override
    protected void reduce(ImmutableBytesWritable key,
            Iterable<ImmutableBytesWritable> values, Context context)
            throws IOException, InterruptedException {
        byte[] keyBytes = key.get();
        for (ImmutableBytesWritable value : values) {
            chunk = Chunk.gbRecord2Chunk(keyBytes, value.get());
            // change to another rock
            if (Bytes.compareTo(lastRockId, chunk.getRockID()) != 0) {
                updateRockDelete();

                lastRockId = chunk.getRockID();
                lastRockDeleteSize = chunk.getSize();

                if (rockIndexWriter != null) {
                    rockIndexWriter.close();
                    rockIndexWriter = null;
                }

                rockIndexWriter = new RockIndexWriter();
                rockIndexWriter.create(PathUtil.getInstance().getGbMetaPath(
                        MD5HashUtil.hexStringFromBytes(lastRockId)));
            } else {
                lastRockDeleteSize += chunk.getSize();
            }

            rockIndexWriter.addChunk(chunk);
            // context.write((ImmutableBytesWritable) key,
            // (ImmutableBytesWritable) value);
        }
        notFirstRock = true;
    }

    private void updateRockDelete() throws IOException {
        if (notFirstRock) {
            RockDB.updateDeleteSize(lastRockId, lastRockDeleteSize);
        }
    }

    /**
     * Advanced application writers can use the
     * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
     * control how the reduce task works.
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKey()) {
            reduce(context.getCurrentKey(), context.getValues(), context);
        }
        cleanup(context);
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // update rock db for the last record, may be duplicate
        updateRockDelete();
        if (rockIndexWriter != null) {
            rockIndexWriter.close();
            rockIndexWriter = null;
        }
    }
}
