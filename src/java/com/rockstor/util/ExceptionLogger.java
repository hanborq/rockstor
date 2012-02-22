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

package com.rockstor.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Logger;

public class ExceptionLogger {
    public static void log(Logger logHandler, Exception e) {
        ByteArrayOutputStream bops = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(bops));
        logHandler.error(bops.toString());
    }

    public static void log(Logger logHandler, Throwable throwable) {
        ByteArrayOutputStream bops = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(bops));
        logHandler.error(bops.toString());
    }

    public static void log(Logger logHandler, String message, Exception e) {
        ByteArrayOutputStream bops = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(bops));
        logHandler.error(message + "\n" + bops.toString());
    }

    public static String getStack(Exception e) {
        ByteArrayOutputStream bops = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(bops));
        return bops.toString();
    }

    private static void f() throws Exception {
        throw new IOException();
    }

    public static void main(String[] args) {
        Logger LOG = Logger.getLogger(ExceptionLogger.class);
        try {
            f();
        } catch (Exception e) {
            System.out.println(getStack(e));
            log(LOG, "exception", e);
        }
    }
}
