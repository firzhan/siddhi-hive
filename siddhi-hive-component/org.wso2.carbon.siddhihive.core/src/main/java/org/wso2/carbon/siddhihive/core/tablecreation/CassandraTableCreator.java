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

import java.util.regex.Pattern;

/**
 * Table to create cassandra table from SiddhiStreamDefinition and execution plan
 */

public final class CassandraTableCreator extends TableCreatorBase {

	private String sCassandraProperties = "";
    private String sCassandraColumns = "";


    public CassandraTableCreator() {
        super();
    }


    public String getQuery() {
        if (listColumns.size() <= 0)
            return null;

        fillHiveFieldString();
        fillCassandraProperties();

	    String sCassandraQuery = "DROP TABLE IF EXISTS " + sDBName + " ;\n";

        sCassandraQuery += ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns +
                ") STORED BY \'org.apache.hadoop.hive.cassandra.CassandraStorageHandler\' WITH SERDEPROPERTIES " +
                "(" + sCassandraProperties +");");


        return sCassandraQuery;
    }


    private void fillCassandraProperties() {
        fillCassandraColumnString();
        String[] streamID = sFullStreamID.split(":");
        sCassandraProperties = ("\"wso2.carbon.datasource.name\" = \""+ Constants.CASSANDRA_DATASOURCE+"\", "
                + "\"cassandra.cf.name\" = \"" + streamID[0].replaceAll(Pattern.quote("."), "_") + "\", "
                + "\"cassandra.columns.mapping\" = \""+sCassandraColumns+"\"");
    }


    private void fillCassandraColumnString() {
        sCassandraColumns = ":key";
        for (int i=0; i<listColumns.size(); i++) {
            sCassandraColumns += (", payload_" + listColumns.get(i).getFieldName());
        }

        sCassandraColumns += (", Timestamp");
    }
}
