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
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Class Handles the sliding length window operations
 */
public class LengthWindowStreamHandler extends WindowStreamHandler {

	private WindowStream windowStream;

    protected String whereClause;
    private String limitClause;

    private String wndSubQueryIdentifier;

	private String firstSelectClause;

	public LengthWindowStreamHandler() {

	}

	/**
	 * Assembles individual elements of hive queries in to a end query for that window
	 *
	 * @param stream                 Length Time Window Stream
	 * @param streamDefinitions      Corresponding stream definition
	 * @return                       Map with corresponding Hive parts. This will be later assembled
	 */
	public Map<String, String> process(Stream stream,
	                                   Map<String, StreamDefinitionExt> streamDefinitions) {

		this.windowStream = (WindowStream) stream;

        String schedulingFreq = String.valueOf(Constants.DEFAULT_LENGTH_WINDOW_FREQUENCY_TIME);
		initializeWndVariables();
        String selectParamsClause = generateWindowSelectClause();
		limitClause = generateLimitStatement();

		invokeGenerateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"
		firstSelectClause = generateFirstSelectClause();

		String fromClause = assembleWindowFromClause();
        Map<String, String> result = new HashMap<String, String>();
		result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);
		result.put(Constants.LENGTH_WINDOW_FREQUENCY, schedulingFreq);

		finalizeWndVariable();
		return result;
	}

	/**
	 * Creates Hive From Clause for the window
	 *
	 * @return From window hive query
	 */
	private String assembleWindowFromClause() {

		if (whereClause.isEmpty()) {
			whereClause = " ";
		}

		Context context = StateManager.getContext();
		String aliasID = context.generateSubQueryIdentifier();
		context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);
		StateManager.setContext(context);

		return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + firstSelectClause + "\n" +
		       whereClause + Constants.CLOSING_BRACT + aliasID;
	}

	/**
	 * Initializes window specific hive variables
	 */
	private void initializeWndVariables() {

		String streamReferenceID = this.windowStream.getStreamReferenceId();
		String streamID = this.windowStream.getStreamId();
		Context context = StateManager.getContext();

		if (streamID.equalsIgnoreCase(streamReferenceID)) {
			wndSubQueryIdentifier = context.generateSubQueryIdentifier();
		} else {
			wndSubQueryIdentifier = streamReferenceID;
		}

		context.setReferenceIDAlias(streamReferenceID, wndSubQueryIdentifier);
/*		context.setWindowStreamProcessingLevel(
				WindowStreamProcessingLevel.LENGTH_WINDOW_PROCESSING);*/
		StateManager.setContext(context);
	}

	/**
	 * Update the ThreadLocal context to be used on separate instance
	 */
	private void finalizeWndVariable() {
		Context context = StateManager.getContext();
		/*context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.NONE);
		context.setWindowProcessingLevel(WindowProcessingLevel.NONE);*/
		StateManager.setContext(context);
	}

	/**
	 * Generate Hive Select Query
	 * @return Hive statement with select part
	 */
	private String generateFirstSelectClause() {
		String clauseIdentifier = wndSubQueryIdentifier;
		String streamReferenceID = this.windowStream.getStreamReferenceId();
		String streamID = this.windowStream.getStreamId();

		if (!streamID.equalsIgnoreCase(streamReferenceID)) {
			clauseIdentifier = streamReferenceID;
		}

		return Constants.SELECT_ALL_FROM_PHRASE + Constants.OPENING_BRACT + Constants.HIVE_SELECT_PARAM_CLAUSE +
                " " + Constants.HIVE_SELECT_CONSTANT_FROM_CLAUSE + this.windowStream.getStreamId()
                + "  " + limitClause + " " + Constants.CLOSING_BRACT + " " + clauseIdentifier;
    }

	/**
	 * Generate where hive query
	 * @param filter Filter object of Siddhi object has the WHere clause and all the other params related to it
	 *
	 */
	public void invokeGenerateWhereClause(Filter filter) {

		Context context = StateManager.getContext();
		//context.setWindowProcessingLevel(WindowProcessingLevel.WND_WHERE_PROCESSING);
		StateManager.setContext(context);
		whereClause = generateWhereClause(filter);
		context = StateManager.getContext();
		//context.setWindowProcessingLevel(WindowProcessingLevel.NONE);
		StateManager.setContext(context);

	}

}
