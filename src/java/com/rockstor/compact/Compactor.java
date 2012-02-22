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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.io.RockCompactWriter;
import com.rockstor.core.io.RockReader;
import com.rockstor.core.io.RockReaderPool;
import com.rockstor.core.io.RockWriter;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Rock;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.RunMode;
import com.rockstor.util.StatusCode;
import com.rockstor.zookeeper.ZK;

/* in compact subdir, files co-exists in such situation:
 1.  null
 not started, or finished
 2. only meta
 not started
 3. meta, data,     
 not started, we created data before index.
 4. meta, data, index
 started, but failed
 5. meta, index
 data had been compacted, but chunk index had not been synchronized
 6. only index
 data had been compacted, and invalid chunks had been removed from chunk db
 need sync left chunk index
 7.  null
 total finished
 */
public class Compactor {
    private static Logger LOG = Logger.getLogger(Compactor.class);
    private FileSystem dfs = null;
    private ArrayList<Rock> deleteRocks = new ArrayList<Rock>();
    private ArrayList<String> deletePaths = new ArrayList<String>();
    private HashMap<String, Long> rockServerGenerationMap;
    private Map<String, Rock> rockMap = new HashMap<String, Rock>();
    private long generation;
    private long lowUsageMark = 50;
    private Configuration conf = null;
    private long totalValidSize = 0;
    private int batchUpdateSize = 1000;
    private int reduceNum = 1;

    private String REDUCE_NUM_CONF = "rockstor.compact.reduce.number";

    private PathUtil pathUtil = null;

    private static Compactor instance = null;

    public static Compactor getInstance() {
        if (instance == null) {
            instance = new Compactor();
        }
        return instance;
    }

    private Compactor() {
        generation = System.currentTimeMillis();
        conf = RockConfiguration.getDefault();
        LOG.info("RockServer " + conf.get("rock.host.name") + " start at "
                + new Date(generation));
        System.setProperty(RunMode.PROPERTY_RUNMODE, RunMode.MODE_MASTERSERVER);
        LOG.info("Set RunMode to "
                + System.getProperty(RunMode.PROPERTY_RUNMODE));
        LOG.info("Init RockConf...");

        LOG.info("Init StatusCode...");
        StatusCode.init();

        LOG.info("Init HDFS Client...");
        RockAccessor.connectHDFS();

        LOG.info("Init HBaseClient...");
        HBaseClient.init();

        LOG.info("Init RockReaderPool...");
        RockReaderPool.getInstance();

        reduceNum = conf.getInt(REDUCE_NUM_CONF, reduceNum);

        pathUtil = PathUtil.getInstance();

        dfs = RockAccessor.getFileSystem();

        lowUsageMark = conf.getLong("rock.compact.lowUsage", lowUsageMark);

        LOG.info("Init Compactor Process OK!");
    }

    private void loadRockServerInfo() throws IOException {
        rockServerGenerationMap = ZK.instance().getRockServerGeneration();
        StringBuilder sb = new StringBuilder();
        sb.append("RockServerGenerationMap\n");
        for (Map.Entry<String, Long> e : rockServerGenerationMap.entrySet()) {
            sb.append(e.getKey() + " : " + e.getValue() + "("
                    + new Date(e.getValue()) + ")\n");
        }
        LOG.info(sb.toString());
    }

    private void scanAllRock() throws IOException {
        rockMap.clear();
        this.deletePaths.clear();
        this.deleteRocks.clear();
        this.totalValidSize = 0;

        LOG.info("Begin scan all Rocks...");
        Map<String, Rock> allRockMap = new HashMap<String, Rock>();
        try {
            allRockMap = RockDB.getRocks();
        } catch (Exception e) {
            LOG.error("Get Rock info from HBase error : " + e);
            ExceptionLogger.log(LOG, e);
            throw new IOException(e);
        }

        loadRockServerInfo();

        Rock rock = null;
        long curGeneration = 0;
        FileStatus status = null;
        String rockID = null;
        String path = null;
        long rockMaxSize = RockWriter.ROCK_MAX_SIZE;
        long usageMax = rockMaxSize / 100;
        String writer = null;

        for (Map.Entry<String, Rock> entry : allRockMap.entrySet()) {
            rock = entry.getValue();
            rockID = rock.getRockID();
            path = Rock.HADOOP_DATA_HOME + "/" + rockID;

            LOG.info("Begin scan Rock(" + rockID + ")...");

            if (!dfs.exists(new Path(path))) {
                LOG.warn("Scanning : Rock(" + rockID
                        + ") in HBase but not in hdfs, ready to delete it.");
                deleteRocks.add(rock);
                continue;
            }

            if (rock.isRetire()) {
                deleteRocks.add(rock);
                deletePaths.add(path);
                continue;
            }

            // file is existed, and contains one chunk at least.
            // not closed
            // if (rock.getOtime() == 0) {
            // check if created by compactor
            writer = rock.getWriter();

            // rock created by compactor, we will check if its file is
            // existed later,
            if (!writer.isEmpty()) { // created by writer worker
                // check if online
                String[] writerInfo = writer.split(":");
                String host = writerInfo[0];
                curGeneration = Long.parseLong(writerInfo[1]);

                if (rockServerGenerationMap.containsKey(host)) {
                    long gen = rockServerGenerationMap.get(host);
                    if (gen == curGeneration) { // writting rock.
                        LOG.info("Scanning : Rock(" + rockID
                                + ") was created by " + rock.getWriter()
                                + ", still in writting, continue.");
                        continue;
                    }
                }
            }

            try {
                status = dfs.getFileStatus(new Path(path));
                if (status.getLen() <= 52) {
                    LOG.warn("Scanning : Rock(" + rockID
                            + ") is an empty rock, ready to delete it.");
                    deletePaths.add(path);
                    deleteRocks.add(rock);
                    continue;
                }
                // calc valid size
                rock.setValidSize(status.getLen() - rock.getDeleteBytes()
                        - rock.getGarbageBytes());

                // ignore rock whose usage is greater than low mark
                if ((rock.getValidSize() / usageMax) > lowUsageMark) {
                    continue;
                }

                if (rock.getValidSize() <= 0) {
                    deletePaths.add(path);
                    deleteRocks.add(rock);
                    LOG.warn("********Scanning : Rock(" + rockID
                            + ") is an empty rock, valid size is less than 0.");
                    continue;
                }

            } catch (Exception e) {
                ExceptionLogger.log(LOG, "Scanning : Rock(" + rockID
                        + ") get file status failed", e);
                rockMap.remove(rockID);
                continue;
            }

            rockMap.put(rockID, rock);
            totalValidSize += rock.getValidSize();
        }
    }

    public boolean verifyCompactResult() throws IOException {
        Path taskDir = new Path(pathUtil.getTaskRootDir());
        FileStatus[] fs = null;

        // check if any task aborted
        if (dfs.exists(taskDir)) {
            fs = dfs.listStatus(taskDir);

            if (fs != null && fs.length > 1) {
                LOG.error("Compact not finished normally, something is wrong");
                throw new IOException(
                        "Compact not finished normally, something is wrong");
            }
        }

        // all tasks finished, then remove garbage chunk index
        Path gbIndexPath = new Path(pathUtil.getGbMetaRootDir());

        dfs.delete(gbIndexPath, true);

        // remove compact work directory
        dfs.delete(new Path(pathUtil.getRootDir()), true);
        return true;
    }

    private void deleteRocks() {
        for (String path : deletePaths) {
            try {
                dfs.delete(new Path(path), true);
                LOG.info("Delete Rock(" + path + ") from hdfs OK.");
            } catch (IOException e) {
                LOG.error("Delete Rock(" + path + ") from hdfs error : " + e);
                ExceptionLogger.log(LOG, e);
            }
        }

        // delete rock from hbase
        try {
            RockDB.remove(deleteRocks);
            LOG.info("Romve rocks from hbase OK.");
        } catch (Exception e) {
            LOG.error("Romve rocks from hbase Error.");
            ExceptionLogger.log(LOG, e);
        }
    }

    private static class TaskAssigner {
        public static class Task implements Comparable<Task> {
            private long size = 0;
            private List<Rock> rocks = new ArrayList<Rock>();
            public static AtomicInteger ms_id = new AtomicInteger(0);
            private int id = 0;

            public Task() {
                id = ms_id.incrementAndGet();
            }

            @Override
            public int compareTo(Task arg0) {
                if (arg0 == this) {
                    return 0;
                }

                Task r = arg0;

                if (size == r.size) {
                    return id - r.id;
                }

                return size < r.size ? -1 : 1;
            }

            @Override
            public boolean equals(Object arg0) {
                if (arg0 == null || !(arg0 instanceof Task)) {
                    return false;
                }
                Task r = (Task) arg0;
                return id == r.id;
            }

            public void add(Rock rock) {
                rocks.add(rock);
                size += rock.getValidSize();
            }

            public List<Rock> getContent() {
                return rocks;
            }
        }

        private Task[] tasks = null;

        public TaskAssigner(int taskNum) {
            assert (taskNum > 0);
            tasks = new Task[taskNum];
            for (int i = 0; i < taskNum; i++) {
                tasks[i] = new Task();
            }
        }

        public void add(Rock rock) {
            tasks[0].add(rock);
            Arrays.sort(tasks, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    return o1.compareTo(o2);
                }
            });
        }

        @SuppressWarnings("unchecked")
        public List<Rock>[] getContent() {
            List<Rock>[] ret = new List[tasks.length];
            for (int i = 0; i < tasks.length; i++) {
                ret[i] = tasks[i].getContent();
            }
            return ret;
        }
    }

    public boolean cleanEnv() throws IOException {
        // remove writting rocks
        scanAllRock();
        deleteRocks();
        return true;
    }

    //
    public boolean calcCompactTask() throws IOException {
        // remove writting rocks
        scanAllRock();
        deleteRocks();

        if (rockMap == null || rockMap.isEmpty() || totalValidSize == 0) {
            return true;
        }

        // can compact to ${taskNum} rocks
        int taskNum = (int) (totalValidSize / RockWriter.ROCK_MAX_SIZE);

        if (taskNum < 1) {
            taskNum = 1;
        }

        TaskAssigner ta = new TaskAssigner(taskNum);
        for (Rock rock : rockMap.values()) {
            ta.add(rock);
        }

        List<Rock>[] tasks = ta.getContent();

        TaskMetaWriter tmr = null;
        String taskIdNamePrefix = "" + generation + "_";
        String taskIdName = null;

        int index = 1;

        // write task meta
        for (List<Rock> task : tasks) {
            taskIdName = taskIdNamePrefix + (index++);
            pathUtil.createSpecTaskDir(dfs, taskIdName);

            tmr = new TaskMetaWriter();
            tmr.create(pathUtil.getTaskMetaPath(taskIdName));
            tmr.putRocks(task);
            tmr.close();
        }

        return true;
    }

    /*
     * meta file path format: dstDir/compact.meta
     */
    public void compactData(String taskIdName) throws IOException,
            NoSuchAlgorithmException {
        Path dstDir = new Path(pathUtil.getSpecTaskDir(taskIdName));
        FileSystem dfs = RockAccessor.getFileSystem();
        if (!dfs.exists(dstDir)) {
            LOG.error("[COMPACTOR]: Directory " + dstDir + " is not exist");
            return;
        }

        String metaFileName = pathUtil.getTaskMetaPath(taskIdName);
        if (!dfs.exists(new Path(metaFileName))) {
            LOG.error("[COMPACTOR]: meta file " + metaFileName
                    + " is not existed");
            return;
        }

        // compact data
        // 1. create rock data file
        String rockIdStr = null;

        // 2. create rock index file

        // 3. load meta file
        TaskMetaReader rocksMeta = new TaskMetaReader();
        rocksMeta.open(metaFileName);
        Map<String, byte[]> rocks = rocksMeta.getRocks();
        rocksMeta.close();
        // 4. compact rock files one by one
        /*
         * for(rock:rocks){ load rock gb from db; load rock gb from delete file
         * sort gb by offset copy chunks to new data file, and write new index,
         * if offset is in gb set, drop and continue }
         */
        Map<Long, Long> gbIndexes = null;
        RockIndexReader rockIndexReader = null;
        RockReader rockReader = null;
        Chunk chunk = null;

        // create rock writer
        RockCompactWriter rockWriter = new RockCompactWriter();
        rockWriter.create(taskIdName);

        rockIdStr = rockWriter.getRockID();

        String dataFileName = pathUtil.getTaskDataPath(taskIdName, rockIdStr);

        String gbIndexPath = null;

        long pos = 0;
        Long size = null;

        for (Entry<String, byte[]> entry : rocks.entrySet()) {
            LOG.info("compacting rock :" + entry.getKey());
        }

        for (Entry<String, byte[]> entry : rocks.entrySet()) {
            gbIndexes = RockDB.getGarbages(entry.getValue());
            rockIndexReader = new RockIndexReader();
            LOG.debug("get " + gbIndexes.size() + " invalid chunks of rock "
                    + entry.getKey() + " from chunk DB");
            gbIndexPath = pathUtil.getGbMetaPath(entry.getKey());
            if (dfs.exists(new Path(gbIndexPath))) {
                rockIndexReader.open(gbIndexPath);

                // merge gb data index
                while (rockIndexReader.hasNext()) {
                    chunk = rockIndexReader.next();
                    LOG.debug("ignore list append chunk: " + chunk);
                    gbIndexes.put(chunk.getOffset(), chunk.getSize()
                            + Chunk.HEADER_LEN);
                }

                rockIndexReader.close();
            }

            // copy chunks and write new index
            rockReader = RockReaderPool.getInstance().get(entry.getKey());
            FSDataInputStream input = rockReader.getFSDataInputStream();
            int pedding_bytes = 0;
            while (rockReader.hasNext()) {
                pos = rockReader.getPos();
                pedding_bytes = (int) (pos & 7);
                if (pedding_bytes != 0) {
                    pos = pos + 8 - pedding_bytes;
                }

                // LOG.info("pos now: "+pos);

                size = gbIndexes.get(pos);

                // ignore deleted chunk
                if (size != null) {
                    LOG.debug("ignore chunk at " + pos + ", size: " + size);

                    rockReader.seekg(pos + size);
                    continue;
                }

                chunk = rockReader.nextChunk();
                if (chunk == null) {
                    LOG.error("[Compactor] read source chunk from "
                            + entry.getKey() + ":" + pos + " Failed");
                    throw new IOException("[Compactor] read source chunk from "
                            + entry.getKey() + ":" + pos + " Failed");
                }

                rockWriter.addChunk(chunk, input);
            }
        }

        rockWriter.close();

        // 5. rename ${compactorDir}/rockId.dat ==> $(rock_data_dir)/rockId
        dfs.rename(new Path(dataFileName), new Path(Rock.HADOOP_DATA_HOME + "/"
                + rockIdStr));

        // 6. remove invalid chunks
        removeInvalidChunks(taskIdName);

        // 7. sync left chunks
        syncLeftChunks(taskIdName);

        // 8. remove task dir
        dfs.delete(dstDir, true);
    }

    public void removeInvalidChunks(String taskIdName) throws IOException,
            NoSuchAlgorithmException {
        // remove invalid chunks
        /*
         * for(rock:rocks){ load rock gb from delete file gen deleted list
         * delete all }
         */
        String taskMetaPath = pathUtil.getTaskMetaPath(taskIdName);

        if (!dfs.exists(new Path(taskMetaPath))) {
            return;
        }

        TaskMetaReader tmr = new TaskMetaReader();
        tmr.open(taskMetaPath);
        Map<String, byte[]> rocks = tmr.getRocks();
        tmr.close();

        String gbIndexPath = null;
        RockIndexReader rockIndexReader = null;
        Chunk chunk = null;
        List<Chunk> chunks = new ArrayList<Chunk>();

        for (Map.Entry<String, byte[]> entry : rocks.entrySet()) {
            gbIndexPath = pathUtil.getGbMetaPath(entry.getKey());

            if (!dfs.exists(new Path(gbIndexPath))) {
                continue;
            }

            rockIndexReader = new RockIndexReader();
            rockIndexReader.open(gbIndexPath);
            while (rockIndexReader.hasNext()) {
                chunk = rockIndexReader.next();
                chunks.add(chunk);
                if (chunks.size() > batchUpdateSize) {
                    ChunkDB.remove(chunks);
                    chunks.clear();
                }
            }

            if (chunks.size() > 0) {
                ChunkDB.remove(chunks);
                chunks.clear();
            }
            rockIndexReader.close();

            // remove gb meta file
            dfs.delete(new Path(gbIndexPath), true);
        }

        // remove task meta file
        dfs.delete(new Path(taskMetaPath), true);

        // retire all compacted rocks
        RockDB.retire(rocks.values());

    }

    public void syncLeftChunks(String taskIdName) throws IOException,
            NoSuchAlgorithmException {
        String newIndexPath = pathUtil.getTaskIndexPath(taskIdName);
        if (!dfs.exists(new Path(newIndexPath))) {
            return;
        }
        RockIndexReader rockIndexReader = null;
        Chunk chunk = null;
        List<Chunk> chunks = new ArrayList<Chunk>();

        rockIndexReader = new RockIndexReader();
        rockIndexReader.open(newIndexPath);
        while (rockIndexReader.hasNext()) {
            chunk = rockIndexReader.next();
            chunks.add(chunk);
            if (chunks.size() > batchUpdateSize) {
                ChunkDB.update(chunks);
                chunks.clear();
            }
        }

        if (chunks.size() > 0) {
            ChunkDB.update(chunks);
        }

        rockIndexReader.close();

        // remove chunk index file
        dfs.delete(new Path(newIndexPath), true);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Compactor inst = Compactor.getInstance();
        try {
            inst.compactData("1306462224682_1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getReduceNum() {
        return reduceNum;
    }

    public void setReduceNum(int reduceNum) {
        this.reduceNum = reduceNum;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
