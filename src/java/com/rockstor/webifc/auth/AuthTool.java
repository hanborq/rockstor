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

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.rockstor.webifc.req.Req;

public class AuthTool {

    private static Logger LOG = Logger.getLogger(AuthTool.class);
    private AuthAlgorithmFactory authFactory = null;

    protected static class AuthData {
        String type;
        String name;
        String signature;
    }

    private static AuthTool instance = null;

    public static AuthTool getInstance() throws AuthException {
        if (instance == null) {
            instance = new AuthTool();
        }
        return instance;
    }

    private AuthTool() throws AuthException {
        authFactory = AuthAlgorithmFactory.getInstance();
    }

    public String auth(Req req) throws AuthException {
        String errMsg = null;
        String authorization = req.getAuthorization();
        if (authorization == null) {
            LOG.info("Auth user OK : anonymous");
            return null;
        }

        AuthData data;
        if ((data = genTypeNameSignature(authorization)) == null) {
            errMsg = "Error Authorization tag : " + authorization;
            LOG.error(errMsg);
            throw new AuthException(errMsg);
        }

        AuthAlgorithmInterface authAlgorithm = authFactory
                .getAuthAlgorithm(data.type);
        if (authAlgorithm == null) {
            errMsg = "No Such Auth Algorithm,Authorization tag : "
                    + authorization;
            LOG.error(errMsg);
            throw new AuthException(errMsg);
        }

        authAlgorithm.doAuth(data, req);

        LOG.info("Auth user OK : " + data.name);
        return data.name;
    }

    private AuthData genTypeNameSignature(String authorization) {

        if (authorization == null)
            return null;

        String[] ss = authorization.split(" ");
        ArrayList<String> l = new ArrayList<String>();
        for (String s : ss) {
            if (!s.trim().equals(""))
                l.add(s);
        }

        if (l.size() != 2) {
            return null;
        }

        AuthData data = new AuthData();
        data.type = l.get(0);

        String[] nameSig = l.get(1).split(":");

        if (nameSig.length != 2) {
            return null;
        }

        data.name = nameSig[0];
        data.signature = nameSig[1];

        return data;
    }

}
