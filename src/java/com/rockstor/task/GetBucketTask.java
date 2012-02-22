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

package com.rockstor.task;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.DateUtil;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.data.CommonPrefixes;
import com.rockstor.webifc.data.Contents;
import com.rockstor.webifc.data.ListBucketResult;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqGetBucket;

public class GetBucketTask extends StateTask<ReqGetBucket> {
    private static Logger LOG = Logger.getLogger(GetBucketTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private static Configuration conf = RockConfiguration.getDefault();
    private static final int DEFAULT_MAX_NUM = 200;
    private static int maxNum = 0;
    static {
        maxNum = conf.getInt("rockstor.bucket.list.maxNum", DEFAULT_MAX_NUM);
        if (maxNum <= 0 || maxNum > DEFAULT_MAX_NUM) {
            maxNum = DEFAULT_MAX_NUM;
        }
    }

    public GetBucketTask(ReqGetBucket req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "GetBucketTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        // 1. get bucket
        String bucketID = req.getBucket();
        Bucket bucketMeta = BucketDB.get(bucketID);

        if (bucketMeta == null) {
            LOG.error("Get Bucket Error: bucket not exist: " + bucketID);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        // check acl
        if (!bucketMeta.canRead(user)) {
            LOG.error("Get Bucket Error: user(" + user
                    + ") does not have read right for bucket(" + bucketID + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        String marker = req.getMarker();
        String prefix = req.getPrefix();
        String delimiter = req.getDelimiter();
        int maxKeys = req.getMaxKeys();

        if (maxKeys > maxNum || maxKeys <= 0) {
            maxKeys = maxNum;
        }

        Pair<ArrayList<Pebble>, ArrayList<String>> p = PebbleDB.scan(bucketID,
                marker, maxKeys, prefix, delimiter);

        ArrayList<Pebble> pebbles = p.getFirst();
        ArrayList<String> prefixs = p.getSecond();

        ListBucketResult lbr = new ListBucketResult();
        lbr.setName(bucketID);

        if (marker != null && !marker.isEmpty()) {
            lbr.setMarker(req.getMarker());
        }

        if (prefix != null && !prefix.isEmpty()) {
            lbr.setPrefix(req.getPrefix());
        }

        if (maxKeys != 0) {
            lbr.setMaxKeys(maxKeys);
            if (pebbles.size() > maxKeys) {
                lbr.setTruncated(true);
                pebbles.remove(pebbles.size() - 1); // drop the last one
            }
        }

        int bucketStrLen = bucketID.length();

        ArrayList<CommonPrefixes> commonPrefixes = new ArrayList<CommonPrefixes>();
        CommonPrefixes cp = null;
        for (String dir : prefixs) {
            cp = new CommonPrefixes();
            cp.setPrefix(dir.substring(bucketStrLen));
            commonPrefixes.add(cp);
        }

        lbr.setCommonPrefixes(commonPrefixes);

        ArrayList<Contents> contents = new ArrayList<Contents>();
        Contents content = null;

        for (Pebble pebble : pebbles) {
            content = new Contents();
            content.setEtag(MD5HashUtil.hexStringFromBytes(pebble.getEtag()));
            content.setKey(pebble.getPebbleID().substring(bucketStrLen));
            content.setLastModified(DateUtil.long2Str(pebble.getVersion()));
            content.setOwner(pebble.getOwner());
            content.setSize(pebble.getSize());

            contents.add(content);
        }

        lbr.setContents(contents);

        XMLData.serialize(lbr, rsp.getOutputStream());
        LOG.info("Get Bucket OK: bucket = " + bucketID);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_GetBucket");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
