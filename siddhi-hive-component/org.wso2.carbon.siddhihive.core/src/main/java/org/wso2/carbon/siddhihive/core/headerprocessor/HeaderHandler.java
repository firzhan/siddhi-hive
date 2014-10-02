/*
 *
 *  *
 *  *  * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package org.wso2.carbon.siddhihive.core.headerprocessor;


import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.JoinStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;

import java.util.Map;

/**
 * Generalized class to handle siddhi stream headers. This will be implemented by other classes
 */
public class HeaderHandler implements StreamHandler {

    SiddhiHiveManager siddhiHiveManager = null;

    public HeaderHandler(SiddhiHiveManager siddhiHiveManager){
       this.siddhiHiveManager = siddhiHiveManager;
    }

	/**
	 *
	 * @param stream                Header Stream
	 * @param streamDefinitions     Corresponding stream definition
	 * @return                       Map with corresponding Header Hive parts. This will be later assembled
	 */
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        Map<String, String> result;
        if (stream instanceof BasicStream) {
            BasicStreamHandler basicStreamHandler = new BasicStreamHandler();
            result = basicStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof WindowStream) {
            WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
            result = windowStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof JoinStream) {
            JoinStreamHandler joinStreamHandler = new JoinStreamHandler();
            result = joinStreamHandler.process(stream, streamDefinitions);
        } else {
            result = null;
        }
        return result;
    }
}
