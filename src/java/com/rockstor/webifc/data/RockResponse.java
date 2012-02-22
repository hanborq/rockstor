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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;

import com.rockstor.util.ExceptionLogger;

@XmlRootElement()
public class RockResponse {
    private static Logger LOG = Logger.getLogger(RockResponse.class);
    private String result;
    private String reason;

    public RockResponse() {
    }

    public RockResponse(String result, String reason) {
        this.result = result;
        this.reason = reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append(" [").append(result)
                .append(", ").append(reason).append("]");
        return sb.toString();
    }

    /**
     * @return the result
     */
    @XmlElement()
    public String getResult() {
        return result;
    }

    /**
     * @param result
     *            the result to set
     */
    public void setResult(String result) {
        this.result = result;
    }

    /**
     * @return the reason
     */
    @XmlElement()
    public String getReason() {
        return reason;
    }

    /**
     * @param reason
     *            the reason to set
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    public static RockResponse deserialize(InputStream input)
            throws JAXBException {
        if (input == null)
            throw new NullPointerException("InputStream instance is null.");

        JAXBContext context = JAXBContext.newInstance(RockResponse.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        Object o = unmarshaller.unmarshal(input);
        return (RockResponse) o;
    }

    public static void serialize(RockResponse response, OutputStream output)
            throws JAXBException {
        if (response == null)
            throw new NullPointerException("Response instance is null.");
        if (output == null)
            throw new NullPointerException("OutputStream instance is null.");

        JAXBContext context = JAXBContext.newInstance(RockResponse.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(response, output);
    }

    public static void sendRockResponseViaHttp(String result, String reason,
            HttpServletResponse resp) {
        RockResponse response = new RockResponse(result, reason);
        resp.setContentType("application/xml; charset=utf-8");
        OutputStream output;
        try {
            output = resp.getOutputStream();
            RockResponse.serialize(response, output);
            output.close();
        } catch (IOException e) {
            ExceptionLogger.log(LOG, e);
        } catch (JAXBException e) {
            ExceptionLogger.log(LOG, e);
        }
    }
}
