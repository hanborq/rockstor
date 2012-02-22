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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import com.rockstor.compact.Compactor;
import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.GarbageChunkDB;
import com.rockstor.core.meta.Chunk;

public class GarbageChunkMapper extends
        TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
    List<byte[]> chunkPrefixes = null;

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        Set<Chunk> chunks = ChunkDB.getAll(key.get());

        if (chunks == null || chunks.isEmpty()) {
            chunkPrefixes.add(value.getRow());
            return;
        }

        ImmutableBytesWritable outKey = null;
        ImmutableBytesWritable outValue = null;
        for (Chunk chunk : chunks) {
            outKey = new ImmutableBytesWritable(Chunk.chunk2GbKey(chunk));
            outValue = new ImmutableBytesWritable(Chunk.chunk2GbValue(chunk));
            context.write(outKey, outValue);
        }
    }

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Compactor.getInstance();
        chunkPrefixes = new ArrayList<byte[]>();
    }

    /**
     * Called once at the end of the task.
     */
    // remove chunk prefixes compacted last time.
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        if (chunkPrefixes.isEmpty()) {
            return;
        }

        GarbageChunkDB.remove(chunkPrefixes);
    }

}
