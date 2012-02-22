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
import org.apache.hadoop.mapreduce.Reducer;

public class GarbageChunkCombine
        extends
        Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
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
        for (ImmutableBytesWritable value : values) {
            context.write(key, value);
        }
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // NOTHING
    }
}
