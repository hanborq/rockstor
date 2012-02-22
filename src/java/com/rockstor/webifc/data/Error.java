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

package com.rockstor.webifc.data;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "Error")
@XmlType(propOrder = { "code", "messgae" })
public class Error extends XMLData {

    private String code;
    private String messgae;

    /**
     * @return the code
     */
    @XmlElement(name = "Code")
    public String getCode() {
        return code;
    }

    /**
     * @param code
     *            the code to set
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * @return the messgae
     */
    @XmlElement(name = "Messgae")
    public String getMessgae() {
        return messgae;
    }

    /**
     * @param messgae
     *            the messgae to set
     */
    public void setMessgae(String messgae) {
        this.messgae = messgae;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Error [code=" + code + ", messgae=" + messgae + "]";
    }
}
