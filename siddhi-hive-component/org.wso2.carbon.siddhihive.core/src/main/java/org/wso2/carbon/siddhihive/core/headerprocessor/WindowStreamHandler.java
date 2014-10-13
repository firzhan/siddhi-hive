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

import java.util.ArrayList;
import java.util.Map;

/**
 * Class responsible for handling and routing all type of window streams
 */
public class WindowStreamHandler extends BasicStreamHandler {

    protected WindowStream windowStream;

	public WindowStreamHandler() {

        Context context = StateManager.getContext();
       // context.setInputStreamProcessingLevel(InputStreamProcessingLevel.WINDOW_STREAM);
        StateManager.setContext(context);
     }

	/**
	 * Based on the window type, further processing is done.
	 *
	 * @param stream             Window Stream
	 * @param streamDefinitions  Corresponding stream definition
	 * @return                    Map with corresponding Hive parts. This will be later assembled
	 */
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
	    WindowStream windowStream = (WindowStream) stream;
	    Object type = windowStream.getWindow().getName();
        if (type.equals(Constants.TIME_WINDOW) || type.equals(Constants.TIME_BATCH_WINDOW)) {
            TimeWindowStreamHandler timeWindowStreamHandler = new TimeWindowStreamHandler();
            return timeWindowStreamHandler.process(windowStream, streamDefinitions);
        } else if (type.equals(Constants.LENGTH_WINDOW)) {
            LengthWindowStreamHandler lengthWindowStreamHandler = new LengthWindowStreamHandler();
            return lengthWindowStreamHandler.process(windowStream, streamDefinitions);
        }else if(type.equals(Constants.LENGTH_BATCH_WINDOW)){
            LengthBatchWindowStreamHandler lengthBatchWindowStreamHandler = new LengthBatchWindowStreamHandler();
            return lengthBatchWindowStreamHandler.process(windowStream, streamDefinitions);
        }

        return null;
    }


    /**
     * Generate Length window select clause
     *
     * @return Hive string
     */
    protected String generateWindowSelectClause() {
        StringBuilder paramsBuilder = new StringBuilder(Constants.SELECT);
        StreamDefinition streamDefinition = windowStream.getStreamDefinition();

        if (streamDefinition != null) {
            ArrayList<Attribute> attributeArrayList =
                    (ArrayList<Attribute>) streamDefinition.getAttributeList();
            String streamID = windowStream.getStreamId();
            String paramValue = "";
            for (Attribute attribute : attributeArrayList) {

                if (paramValue.isEmpty()) {
                    paramValue = "  " + streamID + "." + attribute.getName() + " ";
                } else {
                    paramValue = Constants.COMMA + streamID + "." + attribute.getName() + " ";
                }
            }
            paramsBuilder.append(paramValue);

            String commaValue = Constants.COMMA + streamID + "." + Constants.TIMESTAMPS_COLUMN
                                 + " ";
            paramsBuilder.append(commaValue);
        }

        if (paramsBuilder.length() == 0) {
            paramsBuilder.append(Constants.QUERY_ALL);
        }
        return paramsBuilder.toString();
    }

    /**
     * Add the order by, time stamp and limit clause to the hive query
     * @return Query with LImit, Order BY and TIMESTAMP
     */
    protected String generateLimitStatement(){
        Expression expression = windowStream.getWindow().getParameters()[0];
        IntConstant intConstant = (IntConstant) expression;
        int length = intConstant.getValue();
        String orderBy = Constants.ORDER_BY + "  " + windowStream.getStreamId() + "." +
                         Constants.TIMESTAMPS_COLUMN + "   " + Constants.ASC_ORDER;
        String limit = Constants.LIMIT_PHRASE + String.valueOf(length);
        return orderBy + limit;
    }

}
