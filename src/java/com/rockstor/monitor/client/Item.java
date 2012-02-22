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

package com.rockstor.monitor.client;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class Item {
    private String objName = null;
    private String attrName = null;
    private ObjectName object = null;
    private long interval = 120 * 1000L;
    private String suffix = null;

    public Item() {
    }

    @Override
    public String toString() {
        return String.format("[Item: objName=%s, attr=%s, interval=%d]",
                suffix, attrName, interval);
    }

    public String getObjName() {
        return objName;
    }

    public ObjectName getObject() {
        return object;
    }

    public String getAttrName() {
        return attrName;
    }

    public long getInterval() {
        return interval;
    }

    public String getSuffix() {
        assert (objName != null && !objName.isEmpty());
        return suffix;
    }

    public void setObjName(String objName) {
        assert (objName != null && !objName.isEmpty());
        this.objName = objName;
        try {
            this.object = new ObjectName(objName);
            suffix = objName.substring(objName.lastIndexOf("=") + 1);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    public void setAttrName(String attrName) {
        assert (attrName != null && !attrName.isEmpty());
        this.attrName = attrName;
    }

    public void setInterval(long interval) {
        assert (interval > 1);
        this.interval = interval;
    }
}
