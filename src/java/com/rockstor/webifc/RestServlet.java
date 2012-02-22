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

package com.rockstor.webifc;

import java.io.IOException;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;

import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.io.RockReaderPool;
import com.rockstor.core.io.RockWriterPool;
import com.rockstor.exception.ProcessException;
import com.rockstor.task.StateTask;
import com.rockstor.task.StateTaskFactory;
import com.rockstor.thread.ThreadPoolFactory;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.Host;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.RunMode;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.Req;
import com.rockstor.zookeeper.ZK;

public class RestServlet extends HttpServlet {
    // public static PerfLog pfLog = new PerfLog("../pflog/pf",800,100);
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private static Logger LOG = Logger.getLogger(RestServlet.class);

    private static long generation = 0;
    private static StateTaskFactory stateTaskFactory = StateTaskFactory
            .getInstance();
    private static ThreadPoolFactory threadPoolFactory = ThreadPoolFactory
            .getInstance();

    public static long getGeneration() {
        return generation;
    }

    private static final Configuration conf = RockConfiguration.getDefault();

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.servlet.http.HttpServlet#service(javax.servlet.http.HttpServletRequest
     * , javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void service(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        // generate Worker first.
        Continuation continuation = ContinuationSupport
                .getContinuation(request);

        if (continuation.isExpired()) {
            StatusCode.sendErrorStatusCode(StatusCode.ERR408_TIMEOUT, response);
            request.getInputStream().close();
            response.getOutputStream().close();
            return;
        }

        if (continuation.isInitial()) {
            StateTask<? extends Req> task = null;
            try {
                task = stateTaskFactory.createTask(request, response);
            } catch (ProcessException e) {
                LOG.error("Create Task catch ProcessException : "
                        + ExceptionLogger.getStack(e));
                StatusCode.sendErrorStatusCode(e.getMessage(), response);
                return;
            } catch (Exception e) {
                LOG.error("Create Task catch Other Exception : "
                        + ExceptionLogger.getStack(e));
                StatusCode.sendErrorStatusCode(StatusCode.ERR500_InternalError,
                        response);
                return;
            }

            // it should not run in here...
            if (task == null) {
                LOG.error("Receive error request.");
                StatusCode.sendErrorStatusCode(StatusCode.ERR500_InternalError,
                        response);
                return;
            }

            // long t2 = System.currentTimeMillis();

            // Auth request.
            if (!task.auth()) {
                LOG.error("Auth error.");
                StatusCode.sendErrorStatusCode(
                        StatusCode.ERR403_AccountProblem, response);
                return;
            }

            LOG.info("Request from " + task.getUser() + " : "
                    + task.getReq().toString());
            task.deliver();
        } else {
            LOG.info("@@@@@@@@@@@@@ unexpected route, req: "
                    + request.getPathInfo());
        }
    }

    @Override
    public void destroy() {
        LOG.info("Destroy RockReaderPool...");
        RockReaderPool.getInstance().close();
        LOG.info("Destroy RockWriterPool...");
        RockWriterPool.getInstance().close();
        LOG.info("Destroy HBaseClient...");
        HBaseClient.close();
        LOG.info("Destroy HDFS Client...");
        RockAccessor.disconnectHDFS();
        LOG.info("Destroy ZooKeeper Client...");
        ZK.instance().disconnectFromZooKeeper();

        threadPoolFactory.stopAll();
        LOG.info("All ThreadPools stopped!");

        super.destroy();
        LOG.info("Destroy RestServlet OK!");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.servlet.GenericServlet#init()
     */
    @Override
    public void init() throws ServletException {
        generation = System.currentTimeMillis();
        LOG.info("RockServer " + conf.get("rock.host.name") + " start at "
                + new Date(generation));
        System.setProperty(RunMode.PROPERTY_RUNMODE, RunMode.MODE_ROCKSERVER);
        LOG.info("Set RunMode to "
                + System.getProperty(RunMode.PROPERTY_RUNMODE));
        LOG.info("Init RockConf...");

        LOG.info("Init StatusCode...");
        StatusCode.init();
        LOG.info("Init XMLData...");
        XMLData.init();
        LOG.info("Init ZooKeeper Client...");
        ZK.instance().addRockServer(Host.getHostName(), generation);

        threadPoolFactory.startAll();
        LOG.info("All ThreadPools started!");

        LOG.info("Init HDFS Client...");
        RockAccessor.connectHDFS();
        LOG.info("Init HBaseClient...");
        HBaseClient.init();
        LOG.info("Init RockReaderPool...");
        RockReaderPool.getInstance();
        LOG.info("Init RockWriterPool...");
        RockWriterPool.getInstance();

        super.init();
        LOG.info("Init RestServlet OK!");
    }

}
