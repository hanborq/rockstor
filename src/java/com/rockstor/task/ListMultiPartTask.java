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

import org.apache.hadoop.hbase.util.Pair;
//import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.DateUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.data.CommonPrefixes;
import com.rockstor.webifc.data.ListMultiPartResult;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqListMultiParts;

public class ListMultiPartTask extends StateTask<ReqListMultiParts> {
    // private static Logger LOG = Logger.getLogger(ListMultiPartTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    public ListMultiPartTask(ReqListMultiParts req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "ListMultPartTask",
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
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        // check acl
        if (!bucketMeta.canRead(user)) {
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        String keyMarker = req.getKeyMarker();
        String uploadIdMarker = req.getUploadIdMarker();
        String prefix = req.getPrefix();
        String delimiter = req.getDelimiter();
        int maxUploads = req.getMaxUploads();

        Pair<ArrayList<MultiPartPebble>, ArrayList<String>> p = null;

        p = MultiPartPebbleDB.scan(bucketID, keyMarker, uploadIdMarker,
                maxUploads, prefix, delimiter);

        ArrayList<MultiPartPebble> pebbles = p.getFirst();
        ArrayList<String> prefixs = p.getSecond();

        ListMultiPartResult lmpr = new ListMultiPartResult();
        lmpr.setBucket(bucketID);

        if (keyMarker != null && !keyMarker.isEmpty()) {
            lmpr.setKeyMarker(keyMarker);
        }

        if (uploadIdMarker != null && !uploadIdMarker.isEmpty()) {
            lmpr.setUploadIdMarker(uploadIdMarker);
        }

        if (delimiter != null && !delimiter.isEmpty()) {
            lmpr.setDelimiter(delimiter);
        }

        if (prefix != null && !prefix.isEmpty()) {
            lmpr.setPrefix(prefix);
        }

        if (maxUploads > 0) {
            lmpr.setMaxUploads(maxUploads);
            if (pebbles.size() > maxUploads) {
                lmpr.setTruncated(true);
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

        lmpr.setCommonPrefixes(commonPrefixes);

        ArrayList<ListMultiPartResult.Upload> contents = new ArrayList<ListMultiPartResult.Upload>();
        ListMultiPartResult.Upload content = null;
        String nextKeyMarker = null;
        String nextUploadIdMarker = null;
        long version = 0;
        for (MultiPartPebble pebble : pebbles) {
            content = new ListMultiPartResult.Upload();
            nextKeyMarker = pebble.getPebbleID().substring(bucketStrLen);

            content.setKey(nextKeyMarker);
            version = pebble.getVersion();

            content.setInitiator(DateUtil.long2Str(version));

            nextUploadIdMarker = UploadIdGen.ver2UploadId(pebble.getPebbleID(),
                    version);

            content.setUploadId(nextUploadIdMarker);

            contents.add(content);
        }

        lmpr.setUploads(contents);

        if (lmpr.isTruncated() && nextKeyMarker != null) {
            lmpr.setNextKeyMarker(nextKeyMarker);
            lmpr.setNextUploadIdMarker(nextUploadIdMarker);
        }

        XMLData.serialize(lmpr, rsp.getOutputStream());
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_ListMultiPart");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
