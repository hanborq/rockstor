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

package com.rockstor.core.io;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.rockstor.core.meta.Chunk;

public class ChunkWriter {
    private static final int ALIGN_BYTES = 8;
    private static final int ALIGN_MASK = ALIGN_BYTES - 1;
    private static final byte[] ALGIN_BUF = "0123456789".getBytes();

    private static void align_write(FSDataOutputStream out) throws IOException {
        // aligned by 8 bytes
        long cur_offset = out.getPos();
        int pedding_bytes = (int) (cur_offset & ALIGN_MASK);
        if (pedding_bytes != 0) {
            out.write(ALGIN_BUF, 0, ALIGN_BYTES - pedding_bytes);
        }
    }

    /*
     * format: |rockID|chunkPrefix|partID|timestamp|size|data| 16 16 2 8 8 size
     */
    public static void writeHeader(Chunk chunk, FSDataOutputStream output)
            throws IllegalArgumentException, IOException {
        if (!chunk.valid()) {
            throw new IllegalArgumentException(chunk.toString()
                    + " , some fileds of chunk is invalid!");
        }

        align_write(output);

        chunk.setOffset(output.getPos());
        output.write(chunk.getRockID());
        output.write(chunk.getChunkPrefix());
        output.writeLong(chunk.getTimestamp());
        output.writeLong(chunk.getSize());
        output.writeShort(chunk.getPartID());
        output.writeShort(chunk.getSeqID());
    }

    public static void writeIndex(Chunk chunk, FSDataOutputStream output)
            throws IllegalArgumentException, IOException {
        if (!chunk.valid()) {
            throw new IllegalArgumentException(chunk.toString()
                    + " , some fileds of chunk is invalid!");
        }

        output.writeLong(chunk.getOffset());
        output.write(chunk.getRockID());
        output.write(chunk.getChunkPrefix());
        output.writeShort(chunk.getPartID());
        output.writeShort(chunk.getSeqID());
        output.writeLong(chunk.getTimestamp());
        output.writeLong(chunk.getSize());
    }
}
