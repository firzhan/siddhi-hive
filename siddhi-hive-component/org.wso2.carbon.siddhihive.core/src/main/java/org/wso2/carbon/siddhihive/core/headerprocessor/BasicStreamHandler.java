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
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Generalized class to handle the header part of Siddhi query. This includes Windows, Join Stream etc ..
 */
public class BasicStreamHandler implements StreamHandler {

    public BasicStreamHandler(){
    }

	/**
	 * Starts processing of a given header from this pint onwards
	 *
	 * @param stream              Basic Stream
	 * @param streamDefinitions   Corresponding stream definition
	 * @return                     Map with corresponding Hive parts. This will be later assembled
	 */
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
	    BasicStream basicStream = (BasicStream) stream;
	    String fromClause = generateFromClause(basicStream.getStreamId());
	    String  whereClause = generateWhereClause(basicStream.getFilter());
	    Map<String, String> result = new HashMap<String, String>();
        result.put(Constants.FROM_CLAUSE, fromClause);
        result.put(Constants.WHERE_CLAUSE, whereClause);
        return result;
    }

	/**
	 * Generates From clause for the header
	 *
	 * @param streamId   Stream ID
	 * @return           Stream ID appended with FROM
	 */
    public String generateFromClause(String streamId) {
        return Constants.FROM + " " + streamId;
    }

	/**
	 * Generates where clause of hive query
	 * @param filter  Filter object of the header to generate where clause
	 * @return        Script with where
	 */
	public String generateWhereClause(Filter filter) {
        if (filter != null) {
            ConditionHandler conditionHandler = new ConditionHandler();
            String filterStr = conditionHandler.processCondition(filter.getFilterCondition());
            return Constants.WHERE + " " + filterStr;
        } else {
            return "";
        }

    }
}
