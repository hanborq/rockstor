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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;

public class DateUtil {
    private static SimpleDateFormat sdf = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static Date date = new Date();
    private static TimeZone tz = null;
    private static String defaultTzStr = "Asia/Shanghai";

    static {
        Configuration conf = RockConfiguration.getDefault();
        tz = TimeZone.getTimeZone(conf.get("rockstor.timezone", defaultTzStr));
        sdf.setTimeZone(tz);
    }

    @SuppressWarnings("deprecation")
    public static String toHttpHeader(long time) {
        synchronized (date) {
            date.setTime(time);
            return date.toGMTString();
        }
    }

    public static String toHttpHeader() {
        return toHttpHeader(System.currentTimeMillis());
    }

    // format long to yyyy-MM-dd'T'HH:mm:ss.SSSZ
    public static String long2Str(long time) {
        synchronized (date) {
            date.setTime(time);
            return sdf.format(date);
        }
    }

    // format yyyy-MM-dd'T'HH:mm:ss.SSSZ to long
    @SuppressWarnings("deprecation")
    public static long str2Long(String time) {

        if (time == null || time.isEmpty()) {
            return System.currentTimeMillis();
        }

        synchronized (sdf) {
            try {
                date = sdf.parse(time);
            } catch (ParseException e) {
                return Date.parse(time);
            }
            return date.getTime();
        }
    }

    @SuppressWarnings("deprecation")
    public static void main(String argv[]) {
        long curTime = System.currentTimeMillis();
        String curStr = long2Str(curTime);
        long curTime0 = str2Long(curStr);

        System.out.println(curStr + " " + curTime + " " + curTime0);
        String oldStr = null;
        oldStr = "Tue, 23 Nov 2010 07:35:14 GMT";
        curTime = str2Long(curStr);
        curStr = long2Str(curTime);
        curTime0 = str2Long(curStr);

        System.out.println(curTime + " " + oldStr + "\r\n" + curTime0 + " "
                + curStr);

        System.out.println("------------------------------");

        oldStr = new Date().toGMTString();
        curTime = str2Long(curStr);
        curStr = long2Str(curTime);
        curTime0 = str2Long(curStr);
        System.out.println(curTime + " " + oldStr + "\r\n" + curTime0 + " "
                + curStr);
        System.out.println("------------------------------");
    }
}
