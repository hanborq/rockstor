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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author terry
 * 
 */
public class CompactDirInputSplit extends InputSplit implements Writable {
    private String taskIdName = null;

    public String getTaskIdName() {
        return taskIdName;
    }

    public CompactDirInputSplit() {

    }

    /**
	 * 
	 */
    public CompactDirInputSplit(String taskIdName) {
        this.taskIdName = taskIdName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(taskIdName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        taskIdName = in.readUTF();
    }

}
