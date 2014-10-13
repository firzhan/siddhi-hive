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

package org.wso2.carbon.siddhihive.core.handler;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.query.selection.attribute.ComplexAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.SimpleAttribute;

/**
 *  Attributes of siddhi definitions are handled here. Attributes can be either complex or simple
 */
public class AttributeHandler {

    private ConditionHandler conditionHandler = null;

    public AttributeHandler() {
        conditionHandler = new ConditionHandler();
    }

	/**
	 * Simple attribute types are converted to hive guery like select, having , group by, order by etc.
	 * @param simpleAttribute Siddhi object's simpleAttribute object
	 * @return Hive expression value for particular simpleAttribute
	 */
    public String handleSimpleAttribute(SimpleAttribute simpleAttribute) {
        StringBuilder expressionValueBuilder = new StringBuilder();
        String rename = simpleAttribute.getRename();

        Expression expression = simpleAttribute.getExpression();
        if (expression instanceof Variable) {
            boolean multipleAttr = false;

            if (!rename.trim().isEmpty()) {
                multipleAttr = true;
            }

            Variable variable = (Variable) expression;
            expressionValueBuilder.append(conditionHandler.handleVariable(variable));

            if (!variable.getAttributeName().equals(rename)) {
                expressionValueBuilder.append(Constants.QUERY_AS + rename);
            }

            if (multipleAttr) {
                expressionValueBuilder.append(Constants.COMMA);
            }
        }


        return expressionValueBuilder.toString();
    }

	/**
	 * Complex attribute types are converted to hive guery like count(), avg() etc ...
	 *
	 * @param complexAttribute Siddhi object's complexAttribute object
	 * @return Hive expression value for particular complexAttribute
	 */
    public String handleComplexAttribute(ComplexAttribute complexAttribute) {
        StringBuilder expressionValueBuilder = new StringBuilder();
        String rename = complexAttribute.getRename();
        String complexAttrName = complexAttribute.getAttributeName();

        Expression[] expressions = complexAttribute.getExpressions();

        int expressionLength = expressions.length;

        for (int i = 0; i < expressionLength; i++) {
            Expression expression;
            expression = expressions[i];

            if (expression instanceof Variable) {
                boolean multipleAttr = false;
                if (expressionValueBuilder.length() > 0) {
                    multipleAttr = true;
                }

                Variable variable = (Variable) expression;
                expressionValueBuilder.append(" " + complexAttrName + Constants.OPENING_BRACT +
                        conditionHandler.handleVariable(variable) + Constants.CLOSING_BRACT);

                if (!variable.getAttributeName().equals(rename)) {
                    expressionValueBuilder.append(Constants.QUERY_AS + " " + rename);

                    if (multipleAttr) {
                        expressionValueBuilder.append(Constants.COMMA);
                    }
                }
            }
        }
        return expressionValueBuilder.toString();
    }

}
