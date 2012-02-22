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

package com.rockstor.tools;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.rockstor.core.io.RockAccessor;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.RockConfiguration;
import com.rockstor.zookeeper.ZK;

public class RockStorFsFormat implements Closeable, Tool {
    private static Logger LOG = Logger.getLogger(RockStorFsFormat.class);
    private Configuration conf = null;
    private Configuration hbaseConf = null;

    private Map<String, byte[][]> oldTables = new TreeMap<String, byte[][]>();
    private boolean followOldSplit = false;
    private boolean onlyClean = false;

    private HBaseAdmin ha = null;
    private static final byte[][] emptySplits = new byte[0][0];
    public static String[] tbs = new String[] { "Bucket", "Chunk",
            "GarbageChunk", "MultiPartPebble", "Pebble", "Rock", "User" };

    private static Set<String> rocktbs = new TreeSet<String>(Arrays.asList(tbs));

    public byte[][] getRegionInfo(String tabName) throws IOException {
        HTable table = new HTable(hbaseConf, tabName);
        Map<HRegionInfo, HServerAddress> regionMap = table.getRegionsInfo();
        if (regionMap == null || regionMap.size() < 2) {
            return emptySplits;
        }
        int len = regionMap.size() - 1;
        byte[][] splits = new byte[len][];
        int index = 0;
        for (Map.Entry<HRegionInfo, HServerAddress> entry : regionMap
                .entrySet()) {
            splits[index++] = entry.getKey().getEndKey();
        }
        return splits;
    }

    public RockStorFsFormat() throws IOException {

    }

    /**
     * check if any rockserver is still working.
     * 
     * @return
     * @throws IOException
     */
    public boolean isServerAlive() throws IOException {
        boolean ret = false;
        ZK zk = ZK.instance();

        try {
            zk.connectToZooKeeper();
            ret = !zk.getRockServerGeneration().isEmpty();
        } finally {
            zk.disconnectFromZooKeeper();
        }

        return ret;
    }

    protected void cleanDfs() throws IOException {
        RockAccessor.connectHDFS();
        String rootDir = conf.get("rockstor.rootdir");
        LOG.info("connect to hdfs ok!");
        FileSystem dfs = RockAccessor.getFileSystem();
        dfs.delete(new Path(rootDir), true);
        LOG.info("remove rockstor root dir " + rootDir + " OK!");
        RockAccessor.disconnectHDFS();
        LOG.info("disconnect from hdfs ok!");
    }

    protected void initDfs() throws IOException {
        RockAccessor.connectHDFS();
        String rootDir = conf.get("rockstor.rootdir");
        LOG.info("connect to hdfs ok!");
        FileSystem dfs = RockAccessor.getFileSystem();
        dfs.mkdirs(new Path(rootDir));

        rootDir = conf.get("rockstor.data.home");
        dfs.mkdirs(new Path(rootDir));

        rootDir = conf.get("rockstor.compact.dir");
        dfs.mkdirs(new Path(rootDir));

        LOG.info("init rockstor work dir " + rootDir + " OK!");
        RockAccessor.disconnectHDFS();
        LOG.info("disconnect from hdfs ok!");
    }

    protected void createDB() {
        System.out.println("---- Start creating db----------");

        try {
            // create table
            TableSplitInterface tsi = null;
            String splitClazzName = conf.get("rockstor.db.splits.class");
            System.out.println("rockstor.db.split.class: " + splitClazzName);
            try {
                tsi = (TableSplitInterface) Class.forName(splitClazzName)
                        .newInstance();
            } catch (InstantiationException e) {
                ExceptionLogger.log(LOG, "get table split instance failed", e);
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                ExceptionLogger.log(LOG, "get table split instance failed", e);
                throw new IOException(e);
            } catch (ClassNotFoundException e) {
                ExceptionLogger.log(LOG, "get table split instance failed", e);
                throw new IOException(e);
            }

            Map<String, byte[][]> tbSplitMap = tsi.generateSplits(oldTables);

            HTableGenInterface htgi = null;
            String hTableGenClazzName = conf
                    .get("rockstor.db.descriptor.generator");
            System.out.println("rockstor.db.descriptor.generator: "
                    + hTableGenClazzName);
            try {
                htgi = (HTableGenInterface) Class.forName(hTableGenClazzName)
                        .newInstance();
            } catch (InstantiationException e) {
                ExceptionLogger.log(LOG, "get table generator instance failed",
                        e);
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                ExceptionLogger.log(LOG, "get table generator instance failed",
                        e);
                throw new IOException(e);
            } catch (ClassNotFoundException e) {
                ExceptionLogger.log(LOG, "get table generator instance failed",
                        e);
                throw new IOException(e);
            }

            Map<String, HTableDescriptor> tbDesMap = htgi.initTableDescriptor();
            String tbName = null;

            for (Map.Entry<String, HTableDescriptor> entry : tbDesMap
                    .entrySet()) {
                tbName = entry.getKey();
                ha.createTable(entry.getValue(), tbSplitMap.get(tbName));
                LOG.info("created table " + tbName);
                ha.enableTable(tbName);
                LOG.info("enabled table " + tbName);
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("create rockstor db ok!");
    }

    protected void cleanDB() {
        try {
            HTableDescriptor[] tbDeses = ha.listTables();
            List<String> useTbs = new LinkedList<String>();
            String tbName = null;

            // get table descriptor
            byte[][] splits = null;
            for (HTableDescriptor tbdes : tbDeses) {
                tbName = tbdes.getNameAsString();
                if (rocktbs.contains(tbName)) {
                    useTbs.add(tbName);
                }

                LOG.info("find rocktable " + tbName);
                if (followOldSplit) {
                    splits = getRegionInfo(tbName);
                    oldTables.put(tbName, splits);
                }
            }

            // truncted
            for (String tbname : useTbs) {
                ha.disableTable(tbname);
                LOG.info("disabled table " + tbname + " ok");
                ha.deleteTable(tbname);
                LOG.info("deleted table " + tbname + " ok");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ha != null) {
            }
        }
        LOG.info("clean rockstor db ok!");
    }

    @Override
    public void close() throws IOException {
    }

    public void usage(String message) {
        if (message != null) {
            System.err.println(message);
            System.err.println("");
        }

        System.err.println(getUsage());
    }

    public String getUsage() {
        return USAGE;
    }

    /**
     * @param args
     */
    private final String USAGE = "Usage: format [opts]\n"
            + " where [opts] are:\n"
            + "   --useOldSplits    truncate htable, and split it by the old splits.\n"
            + "   --clean  only clean dfs and hbase, not create table.\n";

    @Override
    public int run(String[] args) {
        Options opt = new Options();
        opt.addOption("useOldSplits", false,
                "truncate htable, and split it by the old splits");
        opt.addOption("clean", false, "only clean system!");

        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opt, args);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            usage(null);
            return -1;
        }

        if (cmd.hasOption("clean")) {
            this.onlyClean = true;
        } else if (cmd.hasOption("useOldSplits")) {
            followOldSplit = true;
            LOG.debug("useOldSplits set to " + followOldSplit);
        }

        if (cmd.getArgList().size() > 0) {
            usage(null);
            return -1;
        }

        try {
            System.out.println(RockConfiguration.str(hbaseConf));
            ha = new HBaseAdmin(hbaseConf);

            cleanDfs();
            cleanDB();

            if (!this.onlyClean) {
                initDfs();
                createDB();
            }
            close();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(RockConfiguration.create(),
                    new RockStorFsFormat(), args);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.hbaseConf = HBaseConfiguration.create(conf);
    }
}
