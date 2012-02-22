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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public abstract class XMLData {

    private static HashMap<Class<? extends XMLData>, JAXBContext> serializeContextMap = new HashMap<Class<? extends XMLData>, JAXBContext>();
    private static HashMap<Class<? extends XMLData>, JAXBContext> deserializeContextMap = new HashMap<Class<? extends XMLData>, JAXBContext>();

    public static void init() {
        try {
            new AccessControlList().registe();
            new ListAllMyBucketsResult().registe();
            new ListBucketResult().registe();
            new Error().registe();

            new InitMultiPartResult().registe();
            new ListPartsResult().registe();
            new CompletePartList().registe();
            new CompleteMultiPartResult().registe();
            new ListMultiPartResult().registe();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    public void registe() throws JAXBException {
        JAXBContext serializeContext = JAXBContext.newInstance(getClass());
        JAXBContext deserializeContext = JAXBContext.newInstance(getClass());
        serializeContextMap.put(getClass(), serializeContext);
        deserializeContextMap.put(getClass(), deserializeContext);
    }

    public static void serialize(XMLData xmlData, OutputStream output)
            throws JAXBException {
        Marshaller marshaller = serializeContextMap.get(xmlData.getClass())
                .createMarshaller();
        marshaller.marshal(xmlData, output);
    }

    public static XMLData deserialize(Class<? extends XMLData> clazz,
            InputStream input) throws JAXBException {
        Unmarshaller unmarshaller = deserializeContextMap.get(clazz)
                .createUnmarshaller();
        XMLData xmlData = (XMLData) unmarshaller.unmarshal(input);
        return xmlData;
    }

}
