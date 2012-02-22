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

import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.data.ListPartsResult;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqListParts;

public class ListPartsTask extends StateTask<ReqListParts> {
    private static Logger LOG = Logger.getLogger(ListPartsTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    public ListPartsTask(ReqListParts req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "ListMultPartTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String object = req.getBucket() + req.getObject();
        long version = UploadIdGen.uploadID2Ver(object, req.getUploadId());

        MultiPartPebble pebble = (MultiPartPebble) MultiPartPebbleDB
                .getPebbleWithVersion(object, version);

        if (pebble == null) {
            LOG.error("Get MultiPart Object List Error: object not exist : "
                    + object);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        // LOG.info("Get pebble ["+ pebble +"], Request User: "+user);

        if (!pebble.canRead(user)) {
            LOG.error("Get MultiPart Object List Error: user(" + user
                    + ") dose not have read right for object(" + object + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        ListPartsResult lpr = new ListPartsResult(pebble, req.getBucket(),
                req.getObject(), req.getMaxParts(), req.getPartNumberMarker());

        XMLData.serialize(lpr, rsp.getOutputStream());
        LOG.info("Get MultiPart Object List OK: object =" + object);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_ListParts");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
