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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opensymphony.xwork2.ActionContext;
import com.opensymphony.xwork2.ActionSupport;

public class UserNoAuthLoginAction extends ActionSupport {

    public static Logger LOG = Logger.getLogger(UserNoAuthLoginAction.class);

    private String username;

    /**
     * @param username
     *            the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.opensymphony.xwork2.ActionSupport#execute()
     */
    @Override
    public String execute() throws Exception {

        User user = new User();
        user.setUserID(username);
        user.setUsername(username);
        user.setDispname(username);
        user.setConsoleAccessKey(username);
        user.setConsoleSecurityKey(username);
        user.setPassword(username);
        user.setRegTime(System.currentTimeMillis());
        user.setActTime(System.currentTimeMillis());

        ActionContext.getContext().getSession().put("USER", user);
        LOG.info("User " + username + " login via simple No Auth Login.");
        return SUCCESS;
    }

}
