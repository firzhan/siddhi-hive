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
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.Conversions;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.JoinStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;

import java.util.HashMap;
import java.util.Map;

/**
 * Class Handles the join operations to create hive script
 */
public class JoinStreamHandler implements StreamHandler {

	public JoinStreamHandler() {
    }

	/**
	 * Function to call the relevant functions to generate hive script for joint operations
	 *
	 * @param stream             Join Header Stream
	 * @param streamDefinitions  Corresponding stream definition
	 * @return                   Map with corresponding Hive parts. This will be later assembled
	 */
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
	    //Process the sub streams
	    JoinStream joinStream = (JoinStream) stream;
        Map<String, String> mapLeftStream = processSubStream(joinStream.getLeftStream(), streamDefinitions);
        Map<String, String> mapRightStream = processSubStream(joinStream.getRightStream(), streamDefinitions);

	    //Handles the condition for each join streams seperately
        ConditionHandler conditionHandler = new ConditionHandler();
        String sCondition = conditionHandler.processCondition(joinStream.getOnCompare());
        String sJoin = Conversions.siddhiToHiveJoin(joinStream.getType());
        String sLeftString = mapLeftStream.get(Constants.FROM_CLAUSE);

        if (sLeftString == null){
	        sLeftString = mapLeftStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        }
        if(sLeftString == null) {
	        sLeftString = mapLeftStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);
        }
        //String manipulation of obtained left and right join string
        sLeftString = sLeftString.replaceFirst(Constants.FROM+" ", "");
        String sRightString = mapRightStream.get(Constants.FROM_CLAUSE);

        if (sRightString == null) {
	        sRightString = mapRightStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        }
        if(sRightString == null) {
	        sRightString = mapRightStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);
        }
        sRightString = sRightString.replaceFirst(Constants.FROM+" ", "");

        String aliasID = "";
        Context context = StateManager.getContext();

        if(joinStream.getLeftStream() instanceof WindowStream){
            WindowStream windowStream = (WindowStream) joinStream.getLeftStream();
            aliasID =  context.generateSubQueryIdentifier();
            context.setReferenceIDAlias(windowStream.getStreamReferenceId(), aliasID);
        }

        if(joinStream.getRightStream() instanceof WindowStream){
            WindowStream windowStream = (WindowStream) joinStream.getRightStream();
            if(aliasID.isEmpty()){
                aliasID =  context.generateSubQueryIdentifier();
            }
            context.setReferenceIDAlias(windowStream.getStreamReferenceId(), aliasID);
        }

        String appendingLeftSelectPhrase = "select * from";
        String appendingRightSelectPhrase;

		// Length batch window query getting appeneded with other string leterals based on the map params
        if(mapLeftStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY) != null){
            appendingLeftSelectPhrase = mapLeftStream.get(Constants.FUNCTION_JOIN_LEFT_CALL_PARAM);
            appendingLeftSelectPhrase = "select *, " + appendingLeftSelectPhrase + "  from";

        }

        if(mapRightStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY) != null){
            appendingRightSelectPhrase = mapRightStream.get(Constants.FUNCTION_JOIN_RIGHT_CALL_PARAM);
            int count = 0;
            for(int i =0; i < sRightString.length(); i++){
                if(sRightString.charAt(i) != '*') {
	                count++;
                }
                else {
	                break;
                }
            }
            String s1 = sRightString.substring(0,count+1);
            String s2 = sRightString.substring(count+1,sRightString.length());
            sRightString =  s1 + " , " + appendingRightSelectPhrase + " " + " as ABC " + s2;
        }

	    String sQuery =
			    "from (  " + appendingLeftSelectPhrase + " " + sLeftString + " " + sJoin + " " +
			    sRightString + " ON   (" + sCondition + ")" + " ) " + aliasID;
	    StateManager.setContext(context);
	    Map<String, String> result = new HashMap<String, String>();

	    //Adding initialization script
        String leftInitializationScript = mapLeftStream.get(Constants.INITALIZATION_SCRIPT);
        String rightInitializationScript = mapRightStream.get(Constants.INITALIZATION_SCRIPT);
        String initializationScript = "";

        if(leftInitializationScript != null) {
	        initializationScript = leftInitializationScript + "\n";
        }

        if(rightInitializationScript != null) {
	        initializationScript += rightInitializationScript + "\n";
        }

        String incrementalClause = "";
        String rightIncrementalClause = mapRightStream.get(Constants.INCREMENTAL_CLAUSE);
        String leftIncrementalClause = mapLeftStream.get(Constants.INCREMENTAL_CLAUSE);

        if(leftIncrementalClause != null) {
	        incrementalClause = leftIncrementalClause + "\n";
        }

        if(rightIncrementalClause != null) {
	        incrementalClause += rightIncrementalClause + "\n";
        }

        if(!initializationScript.isEmpty()) {
	        result.put(Constants.INITALIZATION_SCRIPT, initializationScript);
        }

        if(result.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY) != null) {
	        result.put(Constants.LENGTH_WINDOW_BATCH_FREQUENCY,
	                   result.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY));
        }

        if(result.get(Constants.LENGTH_WINDOW_FREQUENCY) != null) {
	        result.put(Constants.LENGTH_WINDOW_FREQUENCY,
	                   result.get(Constants.LENGTH_WINDOW_FREQUENCY));
        }
        if((!incrementalClause.isEmpty())) {
	        result.put(Constants.INCREMENTAL_CLAUSE, incrementalClause);
        }
        result.put(Constants.JOIN_CLAUSE, sQuery);
        return result;
    }

	/**
	 * If a Join stream has another stream inside it, this method is invoked for that purpose
	 * @param stream             Sub streams with in Join Header Stream
	 * @param streamDefinitions  Corresponding sub stream definition
	 * @return                   Map with corresponding sub stream Hive parts. This will be later assembled
	 */
    private Map<String, String> processSubStream(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        Map<String, String> result;
        if (stream instanceof BasicStream) {
            BasicStreamHandler basicStreamHandler = new BasicStreamHandler();
            result = basicStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof WindowStream) {
            WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
            result = windowStreamHandler.process(stream, streamDefinitions);
        } else {
            result = null;
        }
        return result;
    }
}
