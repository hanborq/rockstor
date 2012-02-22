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
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.rockstor.core.db.UserDB;
import com.rockstor.webifc.data.Bucket;
import com.rockstor.webifc.data.ListAllMyBucketsResult;
import com.rockstor.core.meta.User;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.DateUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqGetService;

public class GetServiceTask extends StateTask<ReqGetService> {
    private static Logger LOG = Logger.getLogger(GetServiceTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    public GetServiceTask(ReqGetService req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "GetServiceTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        if (user == null || user.isEmpty()) {
            LOG.error("Get Service Error: User is null.");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        ListAllMyBucketsResult r = new ListAllMyBucketsResult();
        ArrayList<Bucket> bucketList = new ArrayList<Bucket>();

        User usrInfo = UserDB.get(user);
        if (usrInfo == null) {
            // throw new ProcessException(StatusCode.ERR403_AccessDenied);
        } else {
            Bucket bucket = null;
            for (Entry<String, Long> entry : usrInfo.getBuckets().entrySet()) {
                bucket = new Bucket();
                bucket.setName(entry.getKey());
                bucket.setCreationDate(DateUtil.long2Str(entry.getValue()
                        .longValue()));
                bucketList.add(bucket);
            }
        }

        r.setOwner(user);
        r.setBuckets(bucketList);

        XMLData.serialize(r, rsp.getOutputStream());
        LOG.info("GetService OK.");
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_GetService");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
