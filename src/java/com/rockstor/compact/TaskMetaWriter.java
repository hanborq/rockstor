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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.meta.Rock;

public class TaskMetaWriter implements Closeable {
    private String path;
    private FSDataOutputStream output;

    public TaskMetaWriter() {
    }

    public void create(String path) throws IOException {
        this.path = path;
        FileSystem dfs = RockAccessor.getFileSystem();
        output = dfs.create(new Path(this.path));
    }

    public void putRocks(List<Rock> rocks) throws IOException {
        for (Rock rock : rocks) {
            output.write(rock.getRockMagic());
        }
    }

    @Override
    public void close() throws IOException {
        if (output != null) {
            output.close();
        }
        path = null;
        output = null;
    }
}
