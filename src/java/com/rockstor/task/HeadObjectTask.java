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

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.DateUtil;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.TAG;
import com.rockstor.webifc.req.ReqHeadObject;

public class HeadObjectTask extends StateTask<ReqHeadObject> {
    private static Logger LOG = Logger.getLogger(HeadObjectTask.class);
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

    public HeadObjectTask(ReqHeadObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "HeadObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String object = req.getBucket() + req.getObject();

        Pebble pebble = PebbleDB.get(object);
        if (pebble == null) {
            LOG.error("Head Object Error: obejct not exist: " + object);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (!pebble.canRead(user)) {
            LOG.error("Head Object Error: the user(" + user
                    + ") does not have read right for object(" + object + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // check version instead of modify time, as pebble write once
        boolean download = true;

        long reqTs = 0;
        String tsStr = null;
        tsStr = req.getIfModifiedSince();

        if (tsStr != null) {
            reqTs = DateUtil.str2Long(tsStr);
            download = (reqTs < pebble.getVersion());
            if (download) {
                LOG.info("Head Object: IfModifiedSince " + reqTs
                        + ", current version : " + pebble.getVersion());
            } else {
                LOG.error("Head Object Error: IfModifiedSince " + reqTs
                        + ", current version : " + pebble.getVersion());
            }
        } else {
            tsStr = req.getIfUnmodifiedSince();
            if (tsStr != null) {
                reqTs = DateUtil.str2Long(tsStr);
                download = (pebble.getVersion() < reqTs);
                if (download) {
                    LOG.info("Head Object: IfUnmodifiedSince " + reqTs
                            + ", current version : " + pebble.getVersion());
                } else {
                    LOG.error("Head Object Error: IfUnmodifiedSince " + reqTs
                            + ", current version : " + pebble.getVersion());
                }
            }
        }

        if (!download) {
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        // if match etag
        String etagReal = MD5HashUtil.hexStringFromBytes(pebble.getEtag());
        String etagMatch = null;

        etagMatch = req.getIfMatch();
        if (etagMatch != null) { // has if-match
            if (!etagMatch.equals(etagReal)) {
                download = false; // unnecessary to download
            }
            if (download) {
                LOG.info("Head Object: IfMatch " + etagMatch
                        + ", current etag : " + etagReal);
            } else {
                LOG.error("Head Object Error: IfMatch " + etagMatch
                        + ", current etag : " + etagReal);
            }

        } else {
            etagMatch = req.getIfNoneMatch();
            if (etagMatch != null) {
                if (etagMatch.equals(etagReal)) {
                    download = false; // unnecessary to download
                }
                if (download) {
                    LOG.info("Get Object: IfNoneMatch " + etagMatch
                            + ", current etag : " + etagReal);
                } else {
                    LOG.error("Get Object Error: IfNoneMatch " + etagMatch
                            + ", current etag : " + etagReal);
                }
            }
        }

        if (!download) {
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        // download
        rsp.setContentType(pebble.getMIME());
        rsp.setHeader(TAG.ETAG, etagReal);
        rsp.setHeader(TAG.LAST_MODIFIED, DateUtil.long2Str(pebble.getVersion()));
        rsp.setHeader(TAG.CONTENT_LENGTH, String.valueOf(pebble.getSize()));
        rsp.setHeader("Connection", "close");

        // add meta info
        HashMap<String, String> metas = pebble.getMeta();

        for (Entry<String, String> m : metas.entrySet()) {
            rsp.setHeader(TAG.ROCKSTOR_MATA_ + m.getKey(), m.getValue());
        }

        LOG.info("Head Object OK, obejct = " + object);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_HeadObject");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
