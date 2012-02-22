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

package com.rockstor.webifc.auth;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.util.RockConfiguration;

public class AuthAlgorithmFactory {
    private static Logger LOG = Logger.getLogger(AuthAlgorithmFactory.class);
    private static AuthAlgorithmFactory instance = null;
    private TreeMap<String, AuthAlgorithmInterface> authAlgorithMap = new TreeMap<String, AuthAlgorithmInterface>();

    public static AuthAlgorithmFactory getInstance() throws AuthException {
        if (instance == null) {
            instance = new AuthAlgorithmFactory();
        }
        return instance;
    }

    /**
     * rockstor.auth.types format:
     * identifier:classname[;identifier:classname][;identifier:classname]
     * ROCK0:com.rockstor.webifc.auth.AnonymousAuthAlgorithm;
     * ROCK1:com.rockstor.webifc.auth.SimpleMySqlAuthAlgorithm
     */
    private AuthAlgorithmFactory() throws AuthException {
        Configuration conf = RockConfiguration.getDefault();
        String authList = conf.get("rockstor.auth.types");
        if (authList == null) {
            return;
        }

        authList = authList.trim();

        if (authList.isEmpty()) {
            return;
        }

        String errMsg = null;
        String[] authKVs = authList.split(";");
        String[] authDesc = null;
        String identifier = null;
        String className = null;
        AuthAlgorithmInterface authAlgorithm = null;
        for (String authKV : authKVs) {
            authKV = authKV.trim();
            authDesc = authKV.split(":");
            if (authDesc.length != 2) {
                errMsg = "configure format error, rockstor.auth.types="
                        + authList;
                LOG.fatal(errMsg);
                throw new AuthException(errMsg);
            }

            identifier = authDesc[0].trim();
            className = authDesc[1].trim();

            if (identifier.isEmpty() || className.isEmpty()) {
                errMsg = "configure format error, rockstor.auth.types="
                        + authList;
                LOG.fatal(errMsg);
                throw new AuthException(errMsg);
            }

            if (authAlgorithMap.containsKey(identifier)) {
                errMsg = "configure error, duplicate auth identifiers, rockstor.auth.types="
                        + authList;
                LOG.fatal(errMsg);
                throw new AuthException(errMsg);
            }

            try {
                authAlgorithm = (AuthAlgorithmInterface) Class.forName(
                        className).newInstance();
            } catch (Exception e) {
                errMsg = "configure error, initialize Auth Algorithm Failed, ["
                        + identifier + " : " + className + "], Exception: " + e;
                LOG.fatal(errMsg);
                throw new AuthException(e);
            }
            authAlgorithMap.put(identifier, authAlgorithm);
            LOG.info("initialize Auth Algorithm [" + identifier + " : "
                    + className + "] OK!");
        }
    }

    public AuthAlgorithmInterface getAuthAlgorithm(String type) {
        return authAlgorithMap.get(type);
    }
}
