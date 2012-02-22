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

public class User {

    private String userID;
    private String username;
    private String dispname;
    private String consoleAccessKey;
    private String consoleSecurityKey;
    private String password;
    private long regTime;
    private long actTime;
    /**
     * @return the userID
     */
    public String getUserID() {
        return userID;
    }
    /**
     * @param userID the userID to set
     */
    public void setUserID(String userID) {
        this.userID = userID;
    }
    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }
    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }
    /**
     * @return the dispname
     */
    public String getDispname() {
        return dispname;
    }
    /**
     * @param dispname the dispname to set
     */
    public void setDispname(String dispname) {
        this.dispname = dispname;
    }
    /**
     * @return the consoleAccessKey
     */
    public String getConsoleAccessKey() {
        return consoleAccessKey;
    }
    /**
     * @param consoleAccessKey the consoleAccessKey to set
     */
    public void setConsoleAccessKey(String consoleAccessKey) {
        this.consoleAccessKey = consoleAccessKey;
    }
    /**
     * @return the consoleSecurityKey
     */
    public String getConsoleSecurityKey() {
        return consoleSecurityKey;
    }
    /**
     * @param consoleSecurityKey the consoleSecurityKey to set
     */
    public void setConsoleSecurityKey(String consoleSecurityKey) {
        this.consoleSecurityKey = consoleSecurityKey;
    }
    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }
    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }
    /**
     * @return the regTime
     */
    public long getRegTime() {
        return regTime;
    }
    /**
     * @param regTime the regTime to set
     */
    public void setRegTime(long regTime) {
        this.regTime = regTime;
    }
    /**
     * @return the actTime
     */
    public long getActTime() {
        return actTime;
    }
    /**
     * @param actTime the actTime to set
     */
    public void setActTime(long actTime) {
        this.actTime = actTime;
    }
    
}
