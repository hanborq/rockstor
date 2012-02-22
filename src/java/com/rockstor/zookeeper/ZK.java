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

package com.rockstor.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.RockConfiguration;

public class ZK {

    private static Logger LOG = Logger.getLogger(ZK.class);
    private static Configuration conf = RockConfiguration.getDefault();
    public static final String NODE_ROCKSTORSERVER = "/rockstor/server";

    private static ZK inst = null;
    private ZooKeeper zk;

    public class SessionWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            LOG.info("Session Event : " + event);
        }
    }

    public synchronized static ZK instance() {
        if (inst == null)
            try {
                inst = new ZK();
            } catch (Exception e) {
                LOG.error("Init ZooKeeper Error, exit : " + e);
                System.exit(-1);
            }
        return inst;
    }

    private ZK() throws IOException {
        connectToZooKeeper();
        LOG.info("init ZK OK!");
    }

    public void connectToZooKeeper() throws IOException {
        String address = conf.get("hbase.zookeeper.quorum") + ":"
                + conf.get("hbase.zookeeper.property.clientPort");
        int sessionTimeout = 600000;
        try {
            zk = new ZooKeeper(address, sessionTimeout, new SessionWatcher());
        } catch (IOException e) {
            LOG.error("init ZooKeeper instance error.");
            ExceptionLogger.log(LOG, e);
            throw e;
        }
        LOG.info("zookeeper address : " + address);
    }

    public void disconnectFromZooKeeper() {
        try {
            if (zk != null)
                zk.close();
            LOG.info("Disconnect from ZooKeeper.");
        } catch (InterruptedException e) {
            LOG.info("Disconnect from ZooKeeper catch exception : " + e);
            ExceptionLogger.log(LOG, e);
        }
    }

    private List<ACL> createACL() {
        Id id = new Id("world", "anyone");
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id);
        List<ACL> acl = new ArrayList<ACL>();
        acl.add(acl1);
        return acl;
    }

    private void verifyNodeExist(String path, CreateMode createMode)
            throws IOException, KeeperException, InterruptedException {
        if (path == null) {
            LOG.error("path == null");
            throw new NullPointerException("path == null");
        }
        if (!path.substring(0, 1).equals("/")) {
            LOG.error("path should start with '/' : " + path);
            throw new IOException("path should start with '/' : " + path);
        }

        if (zk.exists(path, false) == null) {

            String[] paths = path.split("/");
            ArrayList<String> pathList = new ArrayList<String>();
            for (String p : paths)
                if (!p.equals(""))
                    pathList.add(p);

            String pathToCreate = "";
            for (String p : pathList) {
                pathToCreate += "/";
                pathToCreate += p;

                if (zk.exists(pathToCreate, false) == null) {
                    zk.create(pathToCreate, "".getBytes(), createACL(),
                            createMode);
                    LOG.info("created node " + pathToCreate);
                }
            }
        } else {
            LOG.info("path " + path + " exist");
        }
    }

    public void addRockServer(String host, long generation) {
        try {
            verifyNodeExist(NODE_ROCKSTORSERVER, CreateMode.PERSISTENT);
            String serverNode = NODE_ROCKSTORSERVER + "/" + host;

            Stat stat = zk.exists(serverNode, null);
            if (stat != null) {
                LOG.error("RockServer " + serverNode
                        + " exist in ZooKeeper already.");

                throw new IOException("RockServer " + serverNode
                        + " exist in ZooKeeper already.");
            }
            zk.create(serverNode, Bytes.toBytes(generation), createACL(),
                    CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("Add RockServer Catch Exception, exit : " + e);
            ExceptionLogger.log(LOG, e);
            System.exit(-1);
        }
    }

    public long getRockServerGeneration(String host) throws IOException {
        String serverNode = NODE_ROCKSTORSERVER + "/" + host;
        Stat stat = null;
        try {
            verifyNodeExist(NODE_ROCKSTORSERVER, CreateMode.PERSISTENT);
            stat = zk.exists(serverNode, null);
            if (stat == null) {
                LOG.error("No Such Node : " + serverNode);
                throw new IOException("No Such Node : " + serverNode);
            }
            byte[] data = zk.getData(serverNode, null, stat);
            return Bytes.toLong(data);
        } catch (Exception e) {
            LOG.error("Get RockServerGeneration catch Exception : " + e);
            ExceptionLogger.log(LOG, e);
            throw new IOException(e);
        }
    }

    public HashMap<String, Long> getRockServerGeneration() throws IOException {
        HashMap<String, Long> nodes = new HashMap<String, Long>();
        try {
            verifyNodeExist(NODE_ROCKSTORSERVER, CreateMode.PERSISTENT);
            List<String> zns = zk.getChildren(NODE_ROCKSTORSERVER, null);
            for (String node : zns) {
                nodes.put(node, getRockServerGeneration(node));
            }
            return nodes;
        } catch (Exception e) {
            LOG.error("Get RockServerGeneration catch Exception : " + e);
            ExceptionLogger.log(LOG, e);
            throw new IOException(e);
        }
    }
    // public boolean isTokenVersionExist(String version) throws
    // KeeperException,
    // InterruptedException
    // {
    // return (null != zk.exists(NODE_TOKEN + "/" + version, false));
    // }
    //
    // public void setTokenVersionList(String version, HashMap<String, String>
    // tokenMap)
    // throws IOException, KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_TOKEN);
    // if (isTokenVersionExist(version))
    // throw new IOException("The version already exist : " + version);
    //
    // Id id = new Id("world", "anyone");
    // ACL acl1 = new ACL(ZooDefs.Perms.ALL,id);
    // List<ACL> acl = new ArrayList<ACL>();
    // acl.add(acl1);
    //
    // zk.create(NODE_TOKEN + "/" + version, "".getBytes(), acl,
    // CreateMode.PERSISTENT);
    // if (logger_.isInfoEnabled())
    // logger_.info("created node " + NODE_TOKEN + "/" + version);
    //
    // for (Entry<String, String> e : tokenMap.entrySet())
    // {
    // zk.create(NODE_TOKEN + "/" + version + "/" + e.getKey(), e
    // .getValue().getBytes(), acl, CreateMode.PERSISTENT);
    // if (logger_.isInfoEnabled())
    // logger_.info("created node " + NODE_TOKEN + "/" + version + "/"
    // + e.getKey() + " : " + e.getValue());
    // }
    // }
    //
    // public void delTokenVersionList(String version) throws IOException,
    // KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_TOKEN + "/" + version);
    //
    // List<String> tokenList = zk.getChildren(NODE_TOKEN + "/" + version,
    // false);
    // for (String t : tokenList)
    // {
    // zk.delete(NODE_TOKEN + "/" + version + "/" + t, -1);
    // if (logger_.isInfoEnabled())
    // logger_.info("delete node " + NODE_TOKEN + "/" + version + "/"
    // + t);
    // }
    // zk.delete(NODE_TOKEN + "/" + version, -1);
    // if (logger_.isInfoEnabled())
    // logger_.info("delete node " + NODE_TOKEN + "/" + version);
    // }
    //
    // public HashMap<String, HashMap<String, String>>
    // getTokenVersionList(boolean regestWatcher)
    // throws IOException, KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_TOKEN);
    // List<String> versions = null;
    // if (regestWatcher)
    // versions = zk.getChildren(NODE_TOKEN, new TokenWatcher());
    // else
    // versions = zk.getChildren(NODE_TOKEN, false);
    //
    // HashMap<String, HashMap<String, String>> ret = new HashMap<String,
    // HashMap<String, String>>();
    // for (String v : versions)
    // {
    // HashMap<String, String> tokenList = new HashMap<String, String>();
    // List<String> tokens = zk.getChildren(NODE_TOKEN + "/" + v, false);
    // for (String host : tokens)
    // {
    // String token = new String(zk.getData(NODE_TOKEN + "/" + v + "/"
    // + host, false, null));
    // tokenList.put(host, token);
    // }
    // ret.put(v, tokenList);
    // }
    //
    // return ret;
    // }
    //
    // public MultiToken getNewToken(boolean regestWatcher)
    // {
    // MultiToken token = null;
    // try
    // {
    // HashMap<String, HashMap<String, String>> versionList =
    // getTokenVersionList(regestWatcher);
    // token = getNewToken(versionList);
    //
    // }
    // catch (IOException e)
    // {
    // logger_.warn("Caught: " + e, e);
    // throw new RuntimeException(e);
    // }
    // catch (KeeperException e)
    // {
    // logger_.warn("Caught: " + e, e);
    // throw new RuntimeException(e);
    // }
    // catch (InterruptedException e)
    // {
    // logger_.warn("Caught: " + e, e);
    // throw new RuntimeException(e);
    // }
    // return token;
    // }
    // /**
    // *
    // * get new token for this node.
    // * all is inefficient, but it's ok, this method is called very rarely.
    // * @param versionList version->(ip, token)
    // * @return
    // */
    // public static MultiToken getNewToken(Map<String, HashMap<String, String>>
    // versionList)
    // {
    // String ip = FBUtilities.getLocalAddress().getHostAddress();
    //
    // Map<String, String> tokenMap = new HashMap<String, String>();
    // for (Map.Entry<String, HashMap<String, String>> entry :
    // versionList.entrySet())
    // {
    // HashMap<String, String> value = entry.getValue();
    // String strToken = value.get(ip);
    // if (strToken != null)
    // {
    // tokenMap.put(entry.getKey(), strToken);
    // }
    // }
    //
    // MultiToken newToken = new MultiToken(tokenMap);
    // return newToken;
    // }
    // public void setCFCItem(String bucket, String udName, long ts) throws
    // IOException, KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_CFC + "/" + bucket);
    // Stat stat = zk.exists(NODE_CFC + "/" + bucket, false);
    // if (stat == null)
    // throw new IOException(NODE_CFC + "/" + bucket + "is null");
    // zk.setData(NODE_CFC + "/" + bucket, new String(udName + ":" +
    // String.valueOf(ts)).getBytes(), stat.getVersion());
    // }
    //
    // // should handler exception here, don't throw out
    // public Pair<String, Long> getCFCItem(String bucket) throws
    // KeeperException, InterruptedException, IOException
    // {
    // verifyNodeExist(NODE_CFC + "/" + bucket);
    // byte[] data = zk.getData(NODE_CFC + "/" + bucket, false, null);
    // String[] datas = new String(data).split(":");
    // if (datas.length != 2)
    // throw new IOException("error data for " + NODE_CFC + "/" + bucket
    // + " : " + new String(data));
    // long ts = Long.parseLong(datas[1]);
    // Pair<String, Long> p = new Pair<String, Long>(datas[0], ts);
    // return p;
    // }
    //
    // public HashMap<String, Pair<String, Long>> getCFCItems()
    // throws IOException, KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_CFC);
    // List<String> buckets = zk.getChildren(NODE_CFC, false);
    // HashMap<String, Pair<String, Long>> ret = new HashMap<String,
    // Pair<String, Long>>();
    // for (String bucket : buckets)
    // {
    // ret.put(bucket, getCFCItem(bucket));
    // }
    // return ret;
    // }
    //
    //
    // public void delCFCItem(String bucket) throws IOException,
    // KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_CFC + "/" + bucket);
    // zk.delete(NODE_CFC + "/" + bucket, -1);
    // }
    //
    // public void addClusterNode(String node) throws IOException,
    // KeeperException,
    // InterruptedException
    // {
    // verifyNodeExist(NODE_CLUSTER);
    // String[] ip_port = node.split(":");
    // String ip = null;
    // String port = null;
    // if (ip_port.length == 1)
    // {
    // ip = ip_port[0];
    // port = "8081";
    // }
    // else if (ip_port.length == 2)
    // {
    // ip = ip_port[0];
    // port = ip_port[1];
    // }
    // else
    // {
    // throw new IOException("host address format error : "+node);
    // }
    // try
    // {
    // InetAddress.getByName(ip);
    // Integer.parseInt(port);
    // }
    // catch (Exception e)
    // {
    // throw new IOException("host address format error : "+node);
    // }
    //
    // Id id = new Id("world", "anyone");
    // ACL acl1 = new ACL(ZooDefs.Perms.ALL,id);
    // List<ACL> acl = new ArrayList<ACL>();
    // acl.add(acl1);
    //
    // zk.create(NODE_CLUSTER + "/" + ip, port.getBytes(), acl,
    // CreateMode.PERSISTENT);
    // }
    //
    // public void delClusterNode(String node) throws IOException,
    // KeeperException,
    // InterruptedException
    // {
    // verifyNodeExist(NODE_CLUSTER + "/" + node);
    // zk.delete(NODE_CLUSTER + "/" + node, -1);
    // }
    //
    // public List<Pair<String, Integer>> getClusterNodes(Watcher watcher)
    // throws IOException, KeeperException, InterruptedException
    // {
    // verifyNodeExist(NODE_CLUSTER);
    // List<String> nodeList = null;
    // if (watcher == null)
    // nodeList = zk.getChildren(NODE_CLUSTER, false);
    // else
    // nodeList = zk.getChildren(NODE_CLUSTER, watcher);
    // List<Pair<String, Integer>> ret = new ArrayList<Pair<String, Integer>>();
    //
    // for (String node : nodeList)
    // {
    // String portString = new String(zk.getData(NODE_CLUSTER + "/" + node,
    // false, null));
    //
    // int port = 8081;
    // try
    // {
    // port = Integer.parseInt(portString);
    // }
    // catch(Exception e)
    // {
    // }
    //
    // Pair<String, Integer> pair = new Pair<String, Integer>(node, port);
    // ret.add(pair);
    // }
    // return ret;
    // }
    //
    // public void setBucketVersion(byte[] data) throws IOException,
    // KeeperException, InterruptedException
    // {
    //
    // verifyNodeExist(NODE_BUCKETVERSION);
    // Stat stat = zk.exists(NODE_BUCKETVERSION, null);
    // zk.setData(NODE_BUCKETVERSION, data, stat.getVersion());
    //
    // }
    // public void setBucketVersion(Map<String, String> b2v)
    // {
    // try
    // {
    // setBucketVersion(getBytesFromBvMap(b2v));
    // }
    // catch (IOException e)
    // {
    // logger_.warn("Caught:"+e,e);
    // throw new RuntimeException(e);
    // }
    // catch (KeeperException e)
    // {
    // logger_.warn("Caught:"+e,e);
    // throw new RuntimeException(e);
    // }
    //
    // catch (InterruptedException e)
    // {
    // logger_.warn("Caught:"+e,e);
    // throw new RuntimeException(e);
    // }
    // }
    // public byte[] getBucketVersion(boolean regestWatcher) throws IOException,
    // KeeperException,
    // InterruptedException
    // {
    // verifyNodeExist(NODE_BUCKETVERSION);
    // if (!regestWatcher)
    // return zk.getData(NODE_BUCKETVERSION, false, null);
    // else
    // return zk.getData(NODE_BUCKETVERSION, new BucketVersionWatcher(), null);
    // }
    // public Map<String, String> getBucketVersionMap(boolean regestWatcher)
    // {
    // byte[] bBv=null;
    // try
    // {
    // bBv = getBucketVersion(regestWatcher);
    // }
    // catch (IOException e)
    // {
    // logger_.warn("Caught: "+e,e);
    // throw new RuntimeException(e);
    //
    // }
    // catch (KeeperException e)
    // {
    // logger_.warn("Caught: "+e,e);
    // throw new RuntimeException(e);
    // }
    // catch (InterruptedException e)
    // {
    // logger_.warn("Caught: "+e,e);
    // throw new RuntimeException(e);
    // }
    //
    // return getBvMap(FBUtilities.utf8String(bBv));
    //
    // }
    //
    // public static byte[] getBytesFromBvMap(Map<String, String>
    // bucketVersionMap)
    // {
    // StringBuilder sb = new StringBuilder();
    //
    // for (Map.Entry<String, String> entry : bucketVersionMap.entrySet())
    // {
    // sb.append(entry.getKey());
    // sb.append(":");
    // sb.append(entry.getValue());
    // sb.append(";");
    // }
    // if(sb.length()==0)
    // return new byte[0];
    // sb.delete(sb.length() - 1, sb.length());
    // byte[] result=FBUtilities.utf8(sb.toString());
    // return result;
    // }
    //
    // public static Map<String, String> getBvMap(String strBv)
    // {
    // Map<String, String> b2vMap = new HashMap<String, String>();
    // if (strBv == null || strBv.equals(""))
    // return b2vMap;
    // String[] tmp = strBv.split(";");
    // for (String item : tmp)
    // {
    // String[] bv = item.split(":");
    // b2vMap.put(bv[0], bv[1]);
    // }
    // return b2vMap;
    // }

}
