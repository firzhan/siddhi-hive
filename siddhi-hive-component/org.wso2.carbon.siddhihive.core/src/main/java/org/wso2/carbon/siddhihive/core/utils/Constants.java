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

/**
 *  Constant values goes here
 *
 */
public final class Constants {
    public final static String TIME_WINDOW = "time";
    public final static String TIME_BATCH_WINDOW = "timeBatch";
    public final static String FROM_TIME = "fromTime";
    public final static String TO_TIME = "toTime";
    public final static String INCREMENTAL_KEYWORD = "@incremental";
    public static final String NAME = "name";
    public static final String TABLE_REFERENCE = "tables";
    public static final String HAS_NON_INDEX_DATA = "hasNonIndexedData";
    public static final String BUFFER_TIME = "bufferTime";

    public static final String FROM = "from";
    public static final String WHERE = "where";
    public static final String JOIN_CLAUSE = "joinClause";
    public static final String FROM_CLAUSE = "fromClause";
    public static final String WHERE_CLAUSE = "whereClause";
    public static final String INCREMENTAL_CLAUSE = "incremental";


    public static final String META = "meta";
    public static final String CORRELATION = "correlation";

    //**********************************************************************************************
    public static final String H_STRING = "STRING";
    public static final String H_INT = "INT";
    public static final String H_DOUBLE = "DOUBLE";
    public static final String H_BINARY = "BINARY";
    public static final String S_STRING = "VARCHAR(128)";
    public static final String S_INT = "INT";
    public static final String S_DOUBLE = "DOUBLE(16,4)";
    public static final String S_BINARY = "BLOB";

    public static final String CASSANDRA_DATASOURCE = "WSO2BAM_CASSANDRA_DATASOURCE";
    public static final String CARBON_DATASOURCE = "WSO2BAM_DATASOURCE";

    public static final String H_LEFT_OUTER_JOIN = "LEFT OUTER JOIN";
    public static final String H_RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN";
    public static final String H_FULL_OUTER_JOIN = "FULL OUTER JOIN";
    public static final String H_JOIN = "JOIN";

    //**********************************************************************************************
    public static final String AND = "AND";
    public static final String OR = "OR";

    public static final String HAVING = " having ";
    public static final String OPENING_BRACT = " ( ";
    public static final String CLOSING_BRACT = " ) ";
    public static final String SPACE = "  ";
    public static final String COMMA = " , ";

    public static final String SELECTION_QUERY = "selectionQuery";
    public static final String GROUP_BY_QUERY = "groupByQuery";
    public static final String HAVING_QUERY = "havingQuery";
    public static final String LENGTH_WIND_FROM_QUERY = "lengthWndFromQuery";
    public static final String LENGTH_BATCH_WIND_FROM_QUERY = "lengthBatchWndFromQuery";
    public static final String LENGTH_WINDOW = "length";
    public static final String LENGTH_BATCH_WINDOW = "lengthBatch";
    public static final String INITALIZATION_SCRIPT= "initialization_Script";
    public static final String ORDER_BY = " ORDER BY ";
    public static final String SELECT = " SELECT ";
    public static final String TIMESTAMPS_COLUMN = "timestamps";

    public static final String TOOL_BOX_DIRECTORY = "siddhi_hive";
    public static final String STREAM_DEF_DIRECTORY = "streamDefn";
    public static final String ANALYTICS_DIRECTORY = "analytics";

    public static final String STREAM_DEF_FILE = "stream_definition";
    public static final String DEFN = "defn";
    public static final String STREAM_DEFINITIONS = "streams.definitions";
    public static final String FILE_NAME = "filename";
    public static final String DEFAULT_USER_NAME = "admin";
    public static final String USER_NAME = "username";
    public static final String DEFAULT_PASSWORD = "admin";
    public static final String PASSWORD = "password";
    public static final String DEFAULT_DESCRIPTION = "This is a sample event stream";
    public static final String DESCRIPTION = "description";
    public static final String ENABLE_INCREMENTAL = "enableIncrementalIndex";
    public static final String PROPERTY_FILE = "streams.properties";
    public static final String ANALYZER_SCRIPTS = "analyzers.scripts";
    public static final String DEFAULT_ANALYZER_DESCRIPTION = "Equivalent hive query for the Siddhi query";
    public static final String SCRIPT_FILE = "converted_hive_script";
    public static final String SCRIPT_NAME = "script";
    public static final String ANALYZER_PROPERTY_FILE = "analyzers.properties";
    public static final String FUNCTION_JOIN_LEFT_CALL_PARAM = "leftFunctionCall";
    public static final String FUNCTION_JOIN_RIGHT_CALL_PARAM = "rightFunctionCall";

    public static final String LENGTH_WINDOW_FREQUENCY = "lengthWndFrequency";
    public static final String LENGTH_WINDOW_BATCH_FREQUENCY = "lengthWndBatchFrequency";
    public static final String DEFAULT_LENGTH_WINDOW_FREQUENCY_TIME = "600";
    public static final String DEFAULT_LENGTH_WINDOW_BATCH_FREQUENCY_TIME = "2000";
    public static final String TIME_WINDOW_FREQUENCY = "timeWindowFrequency";
    public static final String TIME_BATCH_WINDOW_FREQUENCY = "timeBatchWindowFrequency";
    public static final String DEFAULT_SLIDING_FREQUENCY = "10000";

    public static final String ANALYZER_STRING = "analyzer resolvePath(path=\"file://${CARBON_HOME}/repository/components/lib/udf_SiddhiHive.jar\");";
    public static final String HIVE_AUX_JAR    = "set hive.aux.jars.path=${hiveconf:FILE_PATH};";
    public static final String TEMP_FUNCTION   = "create temporary function setCounterAndTimestamp as 'org.wso2.siddhihive.udfunctions.UDFIncrementalCounter';";
    public static final String EXECUTION_INITIALIZER   ="class org.wso2.siddhihive.analytics.ScriptExecutionInitializer;";
    public static final String INITIALIZATION_STATEMENT = ANALYZER_STRING + "\n" + HIVE_AUX_JAR + "\n" + TEMP_FUNCTION + "\n" + EXECUTION_INITIALIZER + "\n";
    public static final String EXECUTION_FINALIZER  ="class org.wso2.siddhihive.analytics.ScriptExecutionFinalizer";
    public static final String CRON = "cron";

    public static final String GROUP_BY = " GROUP BY ";
    public static final String SUB_Q_NUMBER = "subq";
    public static final String QUERY_AS = " AS ";
    public static final String QUERY_ALL = " * ";
    public static final String ASC_ORDER = " ASC ";
    public static final String LIMIT_PHRASE = " LIMIT ";
    public static final String SELECT_ALL_FROM_PHRASE = " SELECT * FROM  ";
    public static final String SELECT_ALL_PHRASE = " SELECT * ";
    public static final String ON_CONDITION = "ON";

    //Hive constants
    public static final String HIVE_SELECT_PARAM_CLAUSE = " selectParamsClause  ";
    public static final String HIVE_SCRIPT_ALIAS = " as ABC";
    public static final String HIVE_SELECT_CONSTANT_FROM_CLAUSE = " Constants.FROM  ";



    public static final String EQUAL_OPERATOR = " = ";
    public static final String NOT_EQUAL_OPERATOR = " != ";
    public static final String GREATER_THAN_OPERATOR = " > ";
    public static final String GREATER_THAN_EQUAL_OPERATOR = " >= ";
    public static final String LESS_THAN_OPERATOR = " < ";
    public static final String LESS_THAN_EQUAL_OPERATOR = " <= ";
    public static final String CONTAINS_OPERATOR = " CONTAINS ";
}