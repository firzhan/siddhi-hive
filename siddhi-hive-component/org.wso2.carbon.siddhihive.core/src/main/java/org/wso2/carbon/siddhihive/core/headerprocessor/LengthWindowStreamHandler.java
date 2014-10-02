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
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Class Handles the sliding length window operations
 */
public class LengthWindowStreamHandler extends WindowStreamHandler {

	private WindowStream windowStream;
	private Map<String, String> result;

	protected String whereClause;
	private String selectParamsClause;
	private String limitClause;
	private String schedulingFreq;

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

		schedulingFreq = String.valueOf(Constants.DEFAULT_LENGTH_WINDOW_FREQUENCY_TIME);
		initializeWndVariables();
		selectParamsClause =
				generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
		limitClause = generateLimitLength();

		invokeGenerateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"
		firstSelectClause = generateFirstSelectClause();

		String fromClause = assembleWindowFromClause();
		result = new HashMap<String, String>();
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
	 * Generate Length window select clause
	 *
	 * @return Hive string
	 */
	private String generateWindowSelectClause() {
		String params = "";
		StreamDefinition streamDefinition = windowStream.getStreamDefinition();

		if (streamDefinition != null) {
			ArrayList<Attribute> attributeArrayList =
					(ArrayList<Attribute>) streamDefinition.getAttributeList();
			String streamID = windowStream.getStreamId();
			for (int i = 0; i < attributeArrayList.size(); ++i) {
				Attribute attribute = attributeArrayList.get(i);

				if (params.isEmpty()) {
					params += "  " + streamID + "." + attribute.getName() + " ";
				} else {
					params += " , " + streamID + "." + attribute.getName() + " ";
				}
			}
			params += ", " + streamID + "." + Constants.TIMESTAMPS_COLUMN + " ";
		}

		if (params.isEmpty()) {
			params = " * ";
		}
		params = Constants.SELECT + "  " + params;
		return params;
	}

	/**
	 * Generate Limit hive querys
	 *
	 * @return Hive query with limit, timestamp and order by tags
	 */
	private String generateLimitLength() {
		Expression expression = windowStream.getWindow().getParameters()[0];
		IntConstant intConstant = (IntConstant) expression;
		int length = intConstant.getValue();

		String orderBY = Constants.ORDER_BY + "  " + windowStream.getStreamId() + "." +
		                 Constants.TIMESTAMPS_COLUMN + "   " + "ASC" + "\n";
		String limit = "LIMIT " + String.valueOf(length) + "\n";
		return orderBY + limit;

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

		return  "SELECT * FROM (" + selectParamsClause + "       " + Constants.FROM + "  " +
		                       this.windowStream.getStreamId() + "  " + limitClause + ")" +
		                       clauseIdentifier;
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
