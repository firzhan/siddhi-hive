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

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Window;

import java.util.HashMap;
import java.util.Map;

/**
 * Class responsible for handling TimeWindowStream
 */
public class TimeWindowStreamHandler extends WindowStreamHandler {
	private WindowStream windowStream;
    private WindowIsolator windowIsolator;

	public TimeWindowStreamHandler() {
        this.windowIsolator = new WindowIsolator();
    }

	/**
	 * Process and fetch the time window related rpoeprties from StreamDefinition.
	 * These properties can be later assembled to generate Hive Query
	 *
	 * @param stream             Time Window Stream
	 * @param streamDefinitions  Corresponding stream definition
	 * @return                   Returns a map with hive script for various parts which can be later used to
	 *                           assemble
	 */
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        this.windowStream = (WindowStream) stream;

        //addStreamReference(this.windowStream.getStreamReferenceId(), this.windowStream.getStreamId());

	    String windowIsolatorClause =
			    generateIsolatorClause(windowStream.getStreamId(), windowStream.getWindow(),
			                           streamDefinitions);
	    String fromClause = generateFromClause(windowStream.getStreamId());
	    String whereClause = generateWhereClause(windowStream.getFilter());
	    String schedulingFreq = generateSchedulingFrequency(windowStream.getWindow());
	    Map<String, String> result = new HashMap<String, String>();
        result.put(Constants.FROM_CLAUSE, fromClause);
        result.put(Constants.WHERE_CLAUSE, whereClause);
        result.put(Constants.INCREMENTAL_CLAUSE, windowIsolatorClause);
        result.put(Constants.TIME_WINDOW_FREQUENCY, schedulingFreq);

        Context context = StateManager.getContext();
        //context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.TIME_WINDOW_PROCESSING);
        StateManager.setContext(context);

        return result;
    }

	/**
	 * Defines the scheduling frequency for the Time batch Window. This achieved by cron jobs
	 *
	 * @param window The window object to define scheduling frequency
	 * @return Hive script with schedule interval
	 */
    private String generateSchedulingFrequency(Window window) {
        if (window.getName().equals(Constants.TIME_WINDOW)) {
            return Constants.DEFAULT_SLIDING_FREQUENCY;
        } else if (window.getName().equals(Constants.TIME_BATCH_WINDOW)) {
            if (window.getParameters()[0] instanceof LongConstant) {
                return String.valueOf(((LongConstant) window.getParameters()[0]).getValue());
            } else if (window.getParameters()[0] instanceof IntConstant) {
                return String.valueOf(((IntConstant) window.getParameters()[0]).getValue());
            }
        }
        return null;
    }

    private String generateIsolatorClause(String streamId, Window window, Map<String, StreamDefinitionExt> streamDefinitions) {
        StreamDefinitionExt streamDefinitionExt = streamDefinitions.get(streamId);
        return windowIsolator.process(windowStream.getWindow(), streamDefinitionExt);
    }
}
