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

import org.apache.log4j.Logger;

import com.opensymphony.xwork2.ActionContext;
import com.opensymphony.xwork2.ActionSupport;

public class SiteInfoAction extends ActionSupport {

    public static Logger LOG = Logger.getLogger(SiteInfoAction.class);

    private String authProtocal;
    private String site;
    private String publicSite;

    /**
     * @return the authProtocal
     */
    public String getAuthProtocal() {
        return authProtocal;
    }

    /**
     * @return the site
     */
    public String getSite() {
        return site;
    }

    /**
     * @return the publicSite
     */
    public String getPublicSite() {
        return publicSite;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.opensymphony.xwork2.ActionSupport#execute()
     */
    @Override
    public String execute() throws Exception {
        User user = (User) ActionContext.getContext().getSession().get("USER");
        LOG.info("User " + user.getUsername() + " request REST URI.");
        authProtocal = System.getProperty("rest.auth.prot");
        site = System.getProperty("rest.site");
        publicSite = System.getProperty("public.site");
        return SUCCESS;
    }

}
