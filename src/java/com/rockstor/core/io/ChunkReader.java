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

import org.apache.hadoop.fs.FSDataInputStream;

import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Rock;

public class ChunkReader {
    private static final int ALIGN_BYTES = 8;
    private static final int ALIGN_MASK = ALIGN_BYTES - 1;

    private static void align_read(FSDataInputStream in, long offset)
            throws IOException {
        int pedding_bytes = (int) (offset & ALIGN_MASK);
        if (pedding_bytes != 0) {
            in.seek(offset + ALIGN_BYTES - pedding_bytes);
        }
    }

    public static Chunk readHeader(FSDataInputStream input, long offset)
            throws IllegalArgumentException, IOException {
        if (offset == 0) {
            throw new IllegalArgumentException("position Error, param offset="
                    + offset);
        }

        align_read(input, offset);

        Chunk chunk = new Chunk();
        chunk.setOffset(input.getPos());

        byte[] rockID = new byte[Rock.ROCK_ID_LEN];
        if (Rock.ROCK_ID_LEN != input.read(rockID)) {
            throw new IOException("read rock magic failed!");
        }

        chunk.setRockID(rockID);

        byte[] chunkPrefix = new byte[Chunk.CHUNK_ID_LEN];
        if (Chunk.CHUNK_ID_LEN != input.read(chunkPrefix)) {
            throw new IOException("read chunk ID failed!");
        }

        chunk.setChunkPrefix(chunkPrefix);
        chunk.setTimestamp(input.readLong());
        chunk.setSize(input.readLong());
        chunk.setPartID(input.readShort());
        chunk.setSeqID(input.readShort());
        return chunk;
    }

    public static Chunk readHeader(FSDataInputStream input)
            throws IllegalArgumentException, IOException {
        return readHeader(input, input.getPos());
    }

    public static long getRockOffset(Chunk chunk, long offset)
            throws IllegalArgumentException {
        if (!chunk.valid()) {
            throw new IllegalArgumentException(chunk.toString()
                    + " , some fileds of pebble is invalid!");
        }

        if (offset > chunk.getSize()) {
            throw new IllegalArgumentException("offset is out of range, "
                    + chunk.toString() + " , offset=" + offset);
        }

        return chunk.getOffset() + offset + Chunk.HEADER_LEN;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
    }

    public static Chunk readIndex(FSDataInputStream input)
            throws IllegalArgumentException, IOException {
        Chunk chunk = new Chunk();

        chunk.setOffset(input.readLong());

        byte[] rockID = new byte[Rock.ROCK_ID_LEN];
        if (Rock.ROCK_ID_LEN != input.read(rockID)) {
            throw new IOException("read rock magic failed!");
        }

        chunk.setRockID(rockID);

        byte[] chunkPrefix = new byte[Chunk.CHUNK_ID_LEN];
        if (Chunk.CHUNK_ID_LEN != input.read(chunkPrefix)) {
            throw new IOException("read chunk ID failed!");
        }

        chunk.setChunkPrefix(chunkPrefix);
        chunk.setPartID(input.readShort());
        chunk.setSeqID(input.readShort());
        chunk.setTimestamp(input.readLong());
        chunk.setSize(input.readLong());

        return chunk;
    }

}
