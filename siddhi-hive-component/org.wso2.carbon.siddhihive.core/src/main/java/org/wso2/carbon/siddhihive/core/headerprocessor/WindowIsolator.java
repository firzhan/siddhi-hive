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
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.query.input.handler.Window;

import java.util.HashMap;
import java.util.Map;

/**
 *  Generates the Window related queries
 */
public class WindowIsolator {

    private String type;
    private Map<String, String> propertyMap;
    private String isolatorClause;

    public WindowIsolator() {
        propertyMap = new HashMap<String, String>();
    }

    public String process(Window window, StreamDefinitionExt streamDefinitionExt) {

        this.type = window.getName();
        if (type.equals(Constants.TIME_WINDOW)) {
            this.populateForTimeWindow(window);
            isolatorClause = this.generateIsolateClause(streamDefinitionExt);
        }

        return isolatorClause;
    }

	/**
	 * Based on the stream definition parameters related to window will be added.
	 *
	 * @param streamDefinitionExt
	 * @return
	 */
    private String generateIsolateClause(StreamDefinitionExt streamDefinitionExt) {//Added this method for future configurations
        int bufferTime = 0;
        String name = streamDefinitionExt.getStreamDefinition().getStreamId() + System.currentTimeMillis();
        return getIncrementalClause(name, streamDefinitionExt.getStreamDefinition().getStreamId(), true, bufferTime);
    }

	/**
	 *Generates the final incremental clause of BAM to do timely processing along with other properties
	 * mentioned in the query stream
	 *
	 * @param name
	 * @param table
	 * @param dataindexing
	 * @param bufferTime
	 * @return
	 */
    public String getIncrementalClause(String name, String table, Boolean dataindexing, int bufferTime) {
        String generalClause = Constants.INCREMENTAL_KEYWORD + "(" + Constants.NAME + "=\"" + name + "\", " + Constants.TABLE_REFERENCE + "=\"" + table + "\", " + Constants.HAS_NON_INDEX_DATA + "=\"" + dataindexing + "\", " + Constants.BUFFER_TIME + "=\"" + bufferTime + "\"";
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(generalClause);
        for (Map.Entry<String, String> entry : propertyMap.entrySet()) {
            stringBuffer.append(", " + entry.getKey() + "=\"" + entry.getValue() + "\"");

        }
        stringBuffer.append(")");
        return stringBuffer.toString();
    }

	/**
	 * Adding time window specific parameters
	 * @param window
	 */
    private void populateForTimeWindow(Window window) {
        propertyMap = new HashMap<String, String>();
        long currentTime = System.currentTimeMillis();
        long duration = 0;
        if (window.getParameters()[0] instanceof LongConstant) {
            duration = ((LongConstant) window.getParameters()[0]).getValue();
        } else if (window.getParameters()[0] instanceof IntConstant) {
            duration = (long) (((IntConstant) window.getParameters()[0]).getValue());
        }
        //long duration = (long) ((LongConstant) window.getParameters()[0]).getValue();
        long toTime = currentTime + duration;
        propertyMap.put(Constants.FROM_TIME, "$NOW-" + duration);
        propertyMap.put(Constants.TO_TIME, "$NOW");
    }
}
