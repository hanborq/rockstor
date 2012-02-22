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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.data.CompleteMultiPartResult;
import com.rockstor.webifc.data.CompletePartList;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqCompleteMultiPart;

public class CompleteMultiPartTask extends StateTask<ReqCompleteMultiPart> {
    private static Logger LOG = Logger.getLogger(CompleteMultiPartTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private MultiPartPebble pebble = null;
    private String id = null;

    public CompleteMultiPartTask(ReqCompleteMultiPart req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "CompleteMultiPartTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        id = req.getBucket() + req.getObject();
        long version = UploadIdGen.uploadID2Ver(id, req.getUploadId());
        pebble = (MultiPartPebble) MultiPartPebbleDB.getPebbleWithVersion(id,
                version);

        if (pebble == null) {
            LOG.error("MultiPart Object Complete Error: object not exist : "
                    + id);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (!user.equals(pebble.getOwner())) {
            LOG.error("MultiPart Object Complete Error: user(" + user
                    + ") is not the owner of object(" + id + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // check parts
        ArrayList<CompletePartList.Part> parts = req.getParts().getParts();
        TreeMap<Short, MultiPartPebble.PartInfo> pim = pebble.getParts();
        if (parts.size() != pim.size()) {
            LOG.error("Put MultiPart Object Complete Error: parts num not match: req size = "
                    + parts.size() + ", mem size = " + pim.size());
            throw new ProcessException(StatusCode.ERR400_InvalidPart);
        }

        MultiPartPebble.PartInfo pi = null;
        short lastPartNumber = -1;
        short curPartNumber = -1;
        String curPartEtag = null;
        for (CompletePartList.Part part : parts) {
            curPartNumber = part.getPartNumber();
            // The list of parts was not in ascending order. Parts list must
            // specified in order by part number.
            if (curPartNumber <= lastPartNumber) {
                LOG.error("Put MultiPart Object Complete Error: part order error : curPartNumber = "
                        + curPartNumber
                        + ", lastPartNumber = "
                        + lastPartNumber);
                throw new ProcessException(StatusCode.ERR400_InvalidPartOrder);
            }

            lastPartNumber = curPartNumber;

            curPartEtag = part.getEtag();
            if (curPartEtag == null || curPartEtag.isEmpty()) {
                LOG.error("Put MultiPart Object Complete Error: empty part etag.");
                throw new ProcessException(StatusCode.ERR400_InvalidPart);
            }

            pi = pim.get(curPartNumber);
            if (pi == null
                    || !curPartEtag.equals(MD5HashUtil
                            .hexStringFromBytes(pi.etag))) {
                LOG.error("Put MultiPart Object Complete Error: error part etag.");
                throw new ProcessException(StatusCode.ERR400_InvalidPart);
            }
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        CompleteMultiPartResult r = new CompleteMultiPartResult();
        Pebble pb = MultiPartPebbleDB.complete(pebble);
        r.setBucket(req.getBucket());
        r.setKey(req.getObject());
        r.setEtag(MD5HashUtil.hexStringFromBytes(pb.getEtag()));

        XMLData.serialize(r, rsp.getOutputStream());
        LOG.info("MultiPart Object Complete OK: " + pb.toString());
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_CompleteMultiPart");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
