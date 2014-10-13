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
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Class Handles the length window batch operations to create hive script
 */
public class LengthBatchWindowStreamHandler extends WindowStreamHandler{

    private String whereClause;
    private String selectParamsClause;
    private String limitClause;

	private String firstSelectClause;
    private String secondSelectClause;
    private String wndSubQueryIdentifier = null;
    private String leftJoinfunctionCall = null;
    private String rightJoinfunctionCall = null;


    public LengthBatchWindowStreamHandler() {
    }

	/**
	 * Function to call the relevant functions to generate hive script for that particular window
	 * @param stream            Length Batch Window Stream
	 * @param streamDefinitions  Corresponding stream definition
	 * @return         Returns a map with hive script for various parts which can be later used to
	 *                 assemble
	 */
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

	    Map<String, String> result = new HashMap<String, String>();
        this.windowStream = (WindowStream) stream;
        initializeWndVariables();
	    String schedulingFreq =
			    String.valueOf(Constants.DEFAULT_LENGTH_WINDOW_BATCH_FREQUENCY_TIME);
	    String initializationScript = generateInitializationScript();
        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateLimitStatement();
        firstSelectClause = generateFirstSelectClause();
        secondSelectClause = generateSecondSelectClause();
        invokeGenerateWhereClause(windowStream.getFilter());
	    String fromClause = assembleWindowFromClause();

        result.put(Constants.LENGTH_BATCH_WIND_FROM_QUERY, fromClause);
        result.put(Constants.INITALIZATION_SCRIPT, initializationScript);

        if(leftJoinfunctionCall != null ) {
	        result.put(Constants.FUNCTION_JOIN_LEFT_CALL_PARAM, leftJoinfunctionCall);
        }

        if(rightJoinfunctionCall != null) {
	        result.put(Constants.FUNCTION_JOIN_RIGHT_CALL_PARAM, rightJoinfunctionCall);
        }
        result.put(Constants.LENGTH_WINDOW_BATCH_FREQUENCY, schedulingFreq);
        return result;
    }



	/**
	 * This function generates the initialization script for hive variables wiht the updaated row
	 * count to be processed.
	 *
	 * When the next time when hive script runs it uses those values to resume running.
	 *
	 * @return Generated hive initialization script
	 */
    private String generateInitializationScript(){
        Context context = StateManager.getContext();
        context.generateTimeStampCounter(true);
        context.generateLimitCounter(true);
        StateManager.setContext(context);

	    Expression expression = windowStream.getWindow().getParameters()[0];
	    IntConstant intConstant = (IntConstant)expression;
	    int length = intConstant.getValue();
	    String maxLimit = "set MAX_LIMIT_COUNT_" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";
	    String totalTimeStampCount = "set TOTAL_TIME_STAMP_COUNT="+ context.generateTimeStampCounter(false)+";" + "\n";

	    return maxLimit + totalTimeStampCount ;
    }

	/**
	 * Initialization of ThreadLocal context variables to be used in later state
	 */
    private void initializeWndVariables(){
        Context context = StateManager.getContext();
        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( streamID.equalsIgnoreCase(streamReferenceID)){
            wndSubQueryIdentifier =  context.generateSubQueryIdentifier();
		}else{
            wndSubQueryIdentifier = streamReferenceID;
		}
	    context.setReferenceIDAlias(streamReferenceID, wndSubQueryIdentifier);
        //context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.LENGTH_BATCH_WINDOW_PROCESSING);
        StateManager.setContext(context);
    }

	/**
	 * Generates the right hand-side of join statement with hive-conf variables
	 * @return generated Hive String
	 */
    private String generateFirstSelectClause(){
        Context context = StateManager.getContext();
        String clauseIdentifier = wndSubQueryIdentifier;
        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( !streamID.equalsIgnoreCase(streamReferenceID)) {
	        clauseIdentifier = streamReferenceID;
        }
	    return "SELECT * " + " FROM (" +
	           selectParamsClause + "       " + Constants.FROM + "  " +
	           this.windowStream.getStreamId() + "  " + " WHERE " + Constants.TIMESTAMPS_COLUMN +
	           " > " +
	           "${hiveconf:TIMESTAMP_TO_BE_PROCESSESED_" + context.generateTimeStampCounter(false) +
	           "}" + "\n" + limitClause + ")" + clauseIdentifier;
    }
   	/**
	 * Generates the left hand-side of join statement with select clause and other query identifier
     * variables
	 * @return generated Hive String
	 */
    private String generateSecondSelectClause(){
        Context context = StateManager.getContext();
        String aliasID = context.generateSubQueryIdentifier();
        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);
        StateManager.setContext(context);
        return "SELECT * FROM ( \n" + this.firstSelectClause  + "\n ) " + aliasID + "\n";
    }

	/**
	 * Generates WHERE part of the hive script
	 * @param filter filter object of Siddhi object has the WHere clause and all the other params related to it
	 */
    public void invokeGenerateWhereClause(Filter filter) {
        Context context;
        context = StateManager.getContext();
        //context.setWindowProcessingLevel(WindowProcessingLevel.WND_WHERE_PROCESSING);
        StateManager.setContext(context);
        whereClause = generateWhereClause(filter);
        context = StateManager.getContext();
        //context.setWindowProcessingLevel(WindowProcessingLevel.NONE);
        StateManager.setContext(context);
    }

	/**
	 * Assembles the hive script from "FROM " clause
	 * ( This excludes hive table creation, cron scheduling etc..)
	 * @return Hive script with From and all the other clause of a window stream
	 */
    private String assembleWindowFromClause(){

        if(whereClause.isEmpty()) {
	        whereClause = " ";
        }

        Context context = StateManager.getContext();
        String aliasID = context.generateSubQueryIdentifier();
        String prveiousAliasID = context.generatePreviousSubQueryIdentifier();
        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);
        StateManager.setContext(context);

		//FUNCTION_JOIN_RIGHT_CALL_PARAM
	    this.leftJoinfunctionCall =
			    " setCounterAndTimestamp( " + context.generateTimeStampCounter(false) + ", " +
			    aliasID + "." + Constants.TIMESTAMPS_COLUMN + " )";
	    this.rightJoinfunctionCall =
			    " setCounterAndTimestamp( " + context.generateTimeStampCounter(false) + ", " +
			    prveiousAliasID + "." + Constants.TIMESTAMPS_COLUMN + " )";

	    return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + secondSelectClause + "\n" +
	           whereClause + Constants.CLOSING_BRACT + aliasID;
    }
}
