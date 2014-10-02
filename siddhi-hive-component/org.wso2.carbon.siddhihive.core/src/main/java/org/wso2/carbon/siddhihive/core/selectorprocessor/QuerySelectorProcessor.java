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

package org.wso2.carbon.siddhihive.core.selectorprocessor;

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.handler.AttributeHandler;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.SelectorProcessingLevel;
import org.wso2.siddhi.query.api.condition.AndCondition;
import org.wso2.siddhi.query.api.condition.Condition;
import org.wso2.siddhi.query.api.condition.OrCondition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.query.selection.Selector;
import org.wso2.siddhi.query.api.query.selection.attribute.ComplexAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.OutputAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.SimpleAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class handles the Selector part of siddhi Query object
 */

public class QuerySelectorProcessor {

    private ConditionHandler conditionHandler = null;
    private AttributeHandler attributeHandler = null;

    private ConcurrentMap<String, String> selectorQueryMap = null;
    private List<SimpleAttribute> simpleAttributeList = null;

	public QuerySelectorProcessor() {

        conditionHandler = new ConditionHandler();
        attributeHandler = new AttributeHandler();

        selectorQueryMap = new ConcurrentHashMap<String, String>();
        simpleAttributeList = new ArrayList<SimpleAttribute>();
	}

	/**
	 * Initiates handling Selector part of Query Object
	 * @param selector Selector object
	 * @return    true in the event of successful completion of selector conversion to siddhi
	 */
    public boolean handleSelector(Selector selector) {

        if (selector == null) {
	        return false;
        }

        selectorQueryMap.clear();

        String selectionQuery = handleSelectionList(selector);
        String groupByQuery = handleGroupByList(selector);
        String handle = handleHavingCondition(selector);

        selectorQueryMap.put(Constants.SELECTION_QUERY, selectionQuery);
        selectorQueryMap.put(Constants.GROUP_BY_QUERY, groupByQuery);
        selectorQueryMap.put(Constants.HAVING_QUERY, handle);

        Context context = StateManager.getContext();

        context.setSelectorProcessingLevel(SelectorProcessingLevel.NONE);
        StateManager.setContext(context);

        return true;
    }

    public ConcurrentMap<String, String> getSelectorQueryMap() {
        return selectorQueryMap;
    }

	/**
	 * Handles and decouples the both Complex and Simple attributes.
	 *
	 * @param selector Selector object
	 * @return Eventual hive string
	 */
    private String handleSelectionList(Selector selector) {

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.SELECTOR);
        StateManager.setContext(context);

        List<OutputAttribute> selectionList = selector.getSelectionList();

        int selectionListSize = selectionList.size();

        if (selectionListSize == 0) {
	        return " ";
        }

        String selectionString = " ";

        for (int i = 0; i < selectionListSize; i++) {

            OutputAttribute outputAttribute = selectionList.get(i);

            postProcessAttributes(outputAttribute);

            if (outputAttribute instanceof SimpleAttribute) {
                selectionString += attributeHandler.handleSimpleAttribute((SimpleAttribute) outputAttribute);
            } else if (outputAttribute instanceof ComplexAttribute) {
                selectionString += attributeHandler.handleComplexAttribute((ComplexAttribute) outputAttribute);
            }

            if ((selectionListSize > 1) && ((i + 1) < selectionListSize))
                selectionString += " , ";
        }
        return selectionString + ", timestamps";
    }

    private void postProcessAttributes(OutputAttribute outputAttribute){

        if(outputAttribute instanceof SimpleAttribute){
            simpleAttributeList.add((SimpleAttribute) outputAttribute);

            String selectionString = attributeHandler.handleSimpleAttribute((SimpleAttribute) outputAttribute);

            Context context = StateManager.getContext();
            context.addSelectionAttributeRename(outputAttribute.getRename(), selectionString);
            StateManager.setContext(context);
      }
    }

	/**
	 * Handles group by operation
	 *
	 * @param selector  Selector object
	 * @return         Group by hive script
	 */
    private String handleGroupByList(Selector selector) {

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.GROUPBY);
        StateManager.setContext(context);

        if( selector.getGroupByList().size() == 0) {
	        return "";
        }
        String groupBy = " GROUP BY ";

        int groupByListSize = simpleAttributeList.size();

        for (int i = 0; i < groupByListSize; i++) {
            SimpleAttribute simpleAttribute = simpleAttributeList.get(i);
            Expression expression = simpleAttribute.getExpression();

            if (expression instanceof Variable) {
                groupBy += "  " + conditionHandler.handleVariable((Variable)expression);

                if ((groupByListSize > 1) && ((i + 1) < groupByListSize))
                    groupBy += " , ";
            }
        }
        return groupBy + ", " + Constants.TIMESTAMPS_COLUMN;
    }

	/**
	 * Handles havings operation
	 *
	 * @param selector  Selector object
	 * @return         Group by hive script
	 */
    private String handleHavingCondition(Selector selector) {

        String handleCondition;
        Condition condition = selector.getHavingCondition();

        if (condition == null) {
	        return " ";
        }

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.HAVING);
        StateManager.setContext(context);

        handleCondition = conditionHandler.processCondition(condition);

        if ((condition instanceof OrCondition) || (condition instanceof AndCondition)) {
	        handleCondition =
			        Constants.OPENING_BRACT + Constants.SPACE + handleCondition + Constants.SPACE +
			        Constants.CLOSING_BRACT;
        }

        return Constants.HAVING + handleCondition;
    }
}
