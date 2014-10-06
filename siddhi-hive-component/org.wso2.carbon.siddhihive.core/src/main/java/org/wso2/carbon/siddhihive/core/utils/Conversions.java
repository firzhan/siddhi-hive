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

package org.wso2.carbon.siddhihive.core.utils;

import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.query.input.JoinStream;

public class Conversions {
    //**********************************************************************************************
    public static String siddhiToHiveType(Attribute.Type type) {
        switch (type) {
            case STRING:
                return Constants.H_STRING;
            case INT:
                return Constants.H_INT;
            case DOUBLE:
                return Constants.H_DOUBLE;
            default:
                return Constants.H_BINARY;
        }
    }

    //**********************************************************************************************
    public static String hiveToSQLType(String sType) {
        if (sType.equals(Constants.H_STRING)) {
            return Constants.S_STRING;
        } else if (sType.equals(Constants.H_INT)) {
            return Constants.S_INT;
        } else if (sType.equals(Constants.H_DOUBLE)) {
            return Constants.S_DOUBLE;
        } else {
            return Constants.S_BINARY;
        }
    }

    //**********************************************************************************************
    public static String siddhiToHiveJoin(JoinStream.Type type) {
        switch (type) {
            case FULL_OUTER_JOIN:
                return Constants.H_FULL_OUTER_JOIN;
            case LEFT_OUTER_JOIN:
                return Constants.H_LEFT_OUTER_JOIN;
            case RIGHT_OUTER_JOIN:
                return Constants.H_RIGHT_OUTER_JOIN;
            case JOIN:
                return Constants.H_JOIN;
            default:
                return Constants.H_JOIN;

        }
    }
}
