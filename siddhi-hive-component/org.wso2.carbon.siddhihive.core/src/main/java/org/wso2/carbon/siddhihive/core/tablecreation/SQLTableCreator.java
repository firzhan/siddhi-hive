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

package org.wso2.carbon.siddhihive.core.tablecreation;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.Conversions;

/**
 * Create SQL table from SiddhiStreamDefinition and execution plan
 */
public final class SQLTableCreator extends TableCreatorBase{

    private String sSQLQuery = "";
    private String sSQLProperties = "";
    private String sSQLColumns = "";

    public SQLTableCreator() {
        super();
    }


    public String getQuery() {
        if (listColumns.size() <= 0)
            return null;

        fillHiveFieldString();
        fillSQLProperties();

        sSQLQuery = ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns
                +") STORED BY \'org.wso2.carbon.hadoop.hive.jdbc.storage.JDBCStorageHandler\' TBLPROPERTIES ("
                +sSQLProperties+");");

        return  sSQLQuery;
    }


    private void fillSQLProperties() {
        fillSQLColumnString();

        sSQLProperties = ("\'wso2.carbon.datasource.name\' = \'"+ Constants.CARBON_DATASOURCE+"\', "
                +"\'hive.jdbc.table.create.query\' = \'CREATE TABLE "+sDBName+"_summary ("+sSQLColumns+")\'");
    }


    private void fillSQLColumnString() {
        sSQLColumns = (listColumns.get(0).getFieldName() + " " + Conversions.hiveToSQLType(listColumns.get(0).getDataType()));
        for (int i = 1; i < listColumns.size(); i++) {
            sSQLColumns += (", " + listColumns.get(i).getFieldName() + " " + Conversions.hiveToSQLType(listColumns.get(i).getDataType()));
        }
    }
}
