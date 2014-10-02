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

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.SelectorProcessingLevel;
import org.wso2.siddhi.query.api.condition.*;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Multiply;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.*;

/**
 *  Class responsible for handling AND, BOOLEAN, OR and NOT condition
 */
public class ConditionHandler {


    public ConditionHandler() {

    }

	/**
	 * Process the conditions in a recursive manner and generates a hive query for that condition
	 *
	 * @param condition Condition object to be processed to obtain the hive String
	 * @return          Condition Hive script
	 */
    public String processCondition(Condition condition) {
        String handleCondition = " ";

        if (condition == null) {
	        return " ";
        }

        if (condition instanceof Compare) {
            handleCondition += handleCompareCondition((Compare) condition);
        } else if (condition instanceof AndCondition) {
            String leftCondition = processCondition(((AndCondition) condition).getLeftCondition());
            String rightCondition = processCondition(((AndCondition) condition).getRightCondition());
            handleCondition += Constants.SPACE + Constants.OPENING_BRACT + leftCondition + Constants.CLOSING_BRACT + Constants.SPACE + Constants.AND + Constants.SPACE + Constants.OPENING_BRACT + rightCondition + Constants.CLOSING_BRACT + Constants.SPACE;
        } else if (condition instanceof BooleanCondition) {
            processCondition(condition);
        } else if (condition instanceof OrCondition) {
            String leftCondition = processCondition(((OrCondition) condition).getLeftCondition());
            String rightCondition = processCondition(((OrCondition) condition).getRightCondition());
            handleCondition += Constants.SPACE + Constants.OPENING_BRACT + leftCondition + Constants.CLOSING_BRACT + Constants.SPACE + Constants.OR + Constants.SPACE + Constants.OPENING_BRACT + rightCondition + Constants.CLOSING_BRACT + Constants.SPACE;
        }

        return handleCondition;

    }

	/**
	 * Handles the comparision conditions by dividing it in to left and right expressions
	 * @param compare Compare object to be processed to obtain the hive String
	 * @return        Comparison Hive script
	 */
    public String handleCompareCondition(Compare compare) {
        String leftExpressiveValue = handleCompareExpression(compare.getLeftExpression());
        String rightExpressiveValue = handleCompareExpression(compare.getRightExpression());
        String operatorString = getOperator(compare.getOperator());

        return " " + leftExpressiveValue + "  " + operatorString + "  " + rightExpressiveValue;
    }

	/**
	 * Handles the comparision of expressions
	 * @param expression expression object to be processed to obtain the hive String
	 * @return           expression Hive script
	 */

    public String handleCompareExpression(Expression expression) {
        String expressionValue = " ";

        if (expression instanceof Variable) {
            expressionValue = handleVariable((Variable) expression);
        } else if (expression instanceof Multiply) {
            Multiply multiply = (Multiply) expression;
            expressionValue = handleCompareExpression(multiply.getLeftValue());
            expressionValue += " * ";
            expressionValue += handleCompareExpression(multiply.getRightValue());
            // expressionValue = ((Multiply)expression.getStreamId() != null ? (variable.getStreamId() + ".") : "") + variable.getAttributeName();
        } else if (expression instanceof Constant) {

            if (expression instanceof IntConstant) {
                IntConstant intConstant = (IntConstant) expression;
                expressionValue = intConstant.getValue().toString();
            } else if (expression instanceof DoubleConstant) {
                DoubleConstant doubleConstant = (DoubleConstant) expression;
                expressionValue = doubleConstant.getValue().toString();
            } else if (expression instanceof FloatConstant) {
                FloatConstant floatConstant = (FloatConstant) expression;
                expressionValue = floatConstant.getValue().toString();
            } else if (expression instanceof LongConstant) {
                LongConstant longConstant = (LongConstant) expression;
                expressionValue = longConstant.getValue().toString();
            }else if (expression instanceof StringConstant) {
                StringConstant stringConstant = (StringConstant) expression;
                expressionValue = "\""+ stringConstant.getValue() + "\" ";
            }
        }

        return expressionValue;
    }

	/**
	 * Handles the Variable of siddhi expressions and generate hive scripts
	 *
	 * @param variable  Variable object to be processed to obtain the hive String
	 * @return          variable hive script
	 */
    public String handleVariable(Variable variable) {
        String variableName ="";
        Context context = StateManager.getContext();

        if( (context.getProcessingLevel() == ProcessingLevel.SELECTOR ) &&
                (context.getSelectorProcessingLevel() == SelectorProcessingLevel.HAVING) ){

            variableName = context.getSelectionAttributeRename(variable.getAttributeName());
            if (variableName == null) {
	            variableName = variable.getAttributeName();
            }
        }else{
            if(variable.getStreamId() != null){
	            if (context.getReferenceIDAlias(variable.getStreamId()) != null) {
	                variableName = context.getReferenceIDAlias(variable.getStreamId()) + ".";
	            } else {
	                variableName = variable.getStreamId() + ".";
	            }
            }
            variableName += variable.getAttributeName();
        }

        StateManager.setContext(context);
        return variableName;
    }

	/**
	 *Obtain the operator to be substituted for hive script
	 * @param operator Conditional operator
	 * @return  Corresponding operator
	 */
    public String getOperator(Condition.Operator operator) {

        if (operator == Condition.Operator.EQUAL)
            return " = ";
        else if (operator == Condition.Operator.NOT_EQUAL)
            return " != ";
        else if (operator == Condition.Operator.GREATER_THAN)
            return " > ";
        else if (operator == Condition.Operator.GREATER_THAN_EQUAL)
            return " >= ";
        else if (operator == Condition.Operator.LESS_THAN)
            return " < ";
        else if (operator == Condition.Operator.LESS_THAN_EQUAL)
            return " <= ";
        else if (operator == Condition.Operator.CONTAINS)
            return " CONTAINS ";

        return " ";
    }
}
