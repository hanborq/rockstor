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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.rockstor.compact.CompactDataTool;
import com.rockstor.compact.Compactor;
import com.rockstor.compact.GenGarbageIndexTool;
import com.rockstor.compact.RecoveryTool;
import com.rockstor.util.RockConfiguration;

public class CompactorTool implements Tool {
    private static Logger LOG = Logger.getLogger(CompactorTool.class);
    private Configuration conf = null;

    /**
     * @param args
     */
    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(RockConfiguration.create(),
                    new CompactorTool(), args);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
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

    private Map<String, BaseOp> opMap = new TreeMap<String, BaseOp>();

    public void registe(BaseOp op) {
        opMap.put(op.getOpName(), op);
    }

    private static abstract class BaseOp {
        private String opName = null;
        private static long preferSleepMs = 120000L; // 2 mins
        static {
            preferSleepMs = RockConfiguration.getDefault().getLong(
                    "rock.compact.sleepBeforeClean", preferSleepMs);
        }

        public BaseOp(String opName) {
            this.opName = opName;
        }

        public String getOpName() {
            return opName;
        }

        public void registe(CompactorTool ct) {
            ct.registe(this);
        }

        public abstract int exec() throws Exception;

        // Step 1. init recovery process with mapreduce
        protected void doRecovery() throws IOException, InterruptedException,
                ClassNotFoundException {
            try {
                if (RecoveryTool.getInstance().run()) {
                    System.out.println("Compact: Recovery OK!");
                } else {
                    System.out.println("Compact: Recovery Failed!");
                    throw new IOException("Compact: Recovery Failed!");
                }
            } catch (Exception e) {
                System.err.println("Compact: Recovery Failed, Exception: " + e);
                throw new IOException("Compact: Recovery Failed", e);
            }
        }

        // Step 2.
        protected void doGenGarbageIndex() throws IOException,
                InterruptedException, ClassNotFoundException {
            try {
                if (GenGarbageIndexTool.getInstance().run()) {
                    System.out
                            .println("Compact: Collect Delete Chunks Information OK!");
                } else {
                    System.out
                            .println("Compact: Collect Delete Chunks Information Failed!");
                    throw new IOException(
                            "Compact: Collect Delete Chunks Information Failed!");
                }
            } catch (Exception e) {
                System.err
                        .println("Compact: Collect Delete Chunks Information Failed, Exception: "
                                + e);
                throw new IOException(
                        "Compact: Collect Delete Chunks Information Failed", e);
            }
        }

        protected void doCompactFull() throws IOException,
                InterruptedException, ClassNotFoundException {
            doRecovery();
            doGenGarbageIndex();
            doGenTaskMeta();
            doCompactTask();
            doVerifyCompactResult();
            System.out.println("now program will clean env after "
                    + preferSleepMs + " ms.");
            Thread.sleep(preferSleepMs);
            doCleanEnv();
        }

        protected void doCompactWithMeta() throws IOException,
                InterruptedException, ClassNotFoundException {
            doCompactTask();
            doVerifyCompactResult();
            System.out.println("now program will clean env after "
                    + preferSleepMs + " ms.");
            Thread.sleep(preferSleepMs);
            doCleanEnv();
        }

        // Step 3.
        protected void doGenTaskMeta() throws IOException {
            try {
                if (Compactor.getInstance().calcCompactTask()) {
                    System.out
                            .println("Compact: Gen Task Meta Information OK!");
                } else {
                    System.out
                            .println("Compact: Gen Task Meta Information Failed!");
                    throw new IOException(
                            "Compact: Gen Task Meta Information Failed!");
                }
            } catch (Exception e) {
                System.err
                        .println("Compact: Gen Task Meta Information Failed, Exception: "
                                + e);
                throw new IOException(
                        "Compact: Gen Task Meta Information Failed", e);
            }
        }

        // Step 4.
        protected void doCompactTask() throws IOException,
                InterruptedException, ClassNotFoundException {
            try {
                if (CompactDataTool.getInstance().run()) {
                    System.out.println("Compact: Compact Data OK!");
                } else {
                    System.out.println("Compact: Compact Data Failed!");
                    throw new IOException("Compact: Compact Data Failed!");
                }
            } catch (Exception e) {
                System.err.println("Compact: Compact Data Failed, Exception: "
                        + e);
                e.printStackTrace();
                throw new IOException("Compact: Compact Data Failed", e);
            }
        }

        // Step 5.
        protected void doVerifyCompactResult() throws IOException {
            try {
                if (Compactor.getInstance().verifyCompactResult()) {
                    System.out.println("Compact: VerifyCompactResult OK!");
                } else {
                    System.out.println("Compact: VerifyCompactResult Failed!");
                    throw new IOException(
                            "Compact: VerifyCompactResult Failed!");
                }
            } catch (Exception e) {
                System.err
                        .println("Compact: VerifyCompactResult Failed, Exception: "
                                + e);
                throw new IOException("Compact: VerifyCompactResult Failed", e);
            }
        }

        // Step 6.
        protected void doCleanEnv() throws IOException {
            try {
                if (Compactor.getInstance().cleanEnv()) {
                    System.out.println("Compact: CleanEnv OK!");
                } else {
                    System.out.println("Compact: CleanEnv Failed!");
                    throw new IOException("Compact: CleanEnv Failed!");
                }
            } catch (Exception e) {
                System.err.println("Compact: CleanEnv Failed, Exception: " + e);
                throw new IOException("Compact: CleanEnv Failed", e);
            }
        }
    }

    private static class CompactWithMetaOp extends BaseOp {
        public CompactWithMetaOp() {
            super("compactWithMeta");
        }

        @Override
        public int exec() throws Exception {
            this.doCompactWithMeta();
            return 0;
        }
    }

    private static class FullCompactOp extends BaseOp {
        public FullCompactOp() {
            super("all");
        }

        @Override
        public int exec() throws Exception {
            this.doCompactFull();
            return 0;
        }
    }

    private static class RecoveryOp extends BaseOp {
        public RecoveryOp() {
            super("recovery");
        }

        @Override
        public int exec() throws Exception {
            this.doRecovery();
            return 0;
        }
    }

    private static class CollectGbOp extends BaseOp {
        public CollectGbOp() {
            super("collectGb");
        }

        @Override
        public int exec() throws Exception {
            this.doGenGarbageIndex();
            return 0;
        }
    }

    private static class CompactTaskGenerator extends BaseOp {
        public CompactTaskGenerator() {
            super("genTask");
        }

        @Override
        public int exec() throws IOException {
            this.doGenTaskMeta();
            return 0;
        }
    }

    private static class DataCompactor extends BaseOp {
        public DataCompactor() {
            super("compactData");
        }

        @Override
        public int exec() throws Exception {
            this.doCompactTask();
            return 0;
        }
    }

    private static class CompactVerifier extends BaseOp {
        public CompactVerifier() {
            super("verify");
        }

        @Override
        public int exec() throws Exception {
            this.doVerifyCompactResult();
            return 0;
        }
    }

    private static class CompactCleaner extends BaseOp {
        public CompactCleaner() {
            super("clean");
        }

        @Override
        public int exec() throws Exception {
            this.doCleanEnv();
            return 0;
        }
    }

    /**
     * @param args
     */
    private final String USAGE = "Usage: compact [opts]\n"
            + " where [opts] are:\n"
            + "   --op = <opTypes> .\n"
            + " <opTypes> are: \n"
            + "          compactWithMeta  compact with previous task meta generated before.\n"
            + "          all              compact. \n"
            + "          recovery         check previous compaction result, and do recovery.\n"
            + "          collectGb        collect garbage information.\n"
            + "          genTask          init compaction task metadata.\n"
            + "          compactData      compact data without clean invalid rock files.\n"
            + "          verify           verify compaction result.\n"
            + "          clean            clean garbages which left by compaction or rockserver. \n"
            + "\n";

    @Override
    public int run(String[] args) throws Exception {
        Options opt = new Options();
        opt.addOption("op", false, "which operation to do");

        CommandLine cmd;
        String opType = "all";
        try {
            cmd = new GnuParser().parse(opt, args);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            usage(null);
            return -1;
        }

        if (cmd.hasOption("op")) {
            opType = cmd.getOptionValue("op", opType);
            LOG.debug("opType set to " + opType);
        }

        BaseOp op = opMap.get(opType);
        if (cmd.getArgList().size() > 0 || op == null) {
            usage("unknown op type: " + opType);
            return -1;
        }

        return op.exec();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public CompactorTool() {
        new CompactWithMetaOp().registe(this);
        new FullCompactOp().registe(this);
        new RecoveryOp().registe(this);
        new CollectGbOp().registe(this);
        new CompactTaskGenerator().registe(this);
        new DataCompactor().registe(this);
        new CompactVerifier().registe(this);
        new CompactCleaner().registe(this);
    }
}
