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

package com.bigdata.rockstor.console;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;

public class InitServlet extends HttpServlet {

    public static Logger LOG = Logger.getLogger(InitServlet.class);

    /*
     * (non-Javadoc)
     * 
     * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
     */
    @Override
    public void init(ServletConfig config) throws ServletException {

        LOG.info("Init IninServlet, set default HostnameVerifier.");

        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            public boolean verify(String urlHostName, SSLSession session) {
                System.out.println("Warning: URL Host: " + urlHostName
                        + " vs. " + session.getPeerHost());
                return true;
            }
        });

        String site = config.getInitParameter("rest.site");
        if (site == null || "".equals(site)) {
            LOG.error("Not set rest.site, exit.");
            System.exit(-1);
        } else {
            System.setProperty("rest.site", site);
            LOG.info("Get rest.site = " + site);
        }
        site = config.getInitParameter("public.site");
        if (site == null || "".equals(site)) {
            LOG.error("Not set public.site, exit.");
            System.exit(-1);
        } else {
            System.setProperty("public.site", site);
            LOG.info("Get public.site = " + site);
        }
        String authProtocal = config.getInitParameter("rest.auth.prot");
        if (authProtocal == null || "".equals(authProtocal)) {
            LOG.error("Not set rest.auth.prot, exit.");
            System.exit(-1);
        } else {
            System.setProperty("rest.auth.prot", authProtocal);
            LOG.info("Get rest.auth.prot = " + authProtocal);
        }
    }
}
