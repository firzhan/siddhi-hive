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

package org.wso2.carbon.siddhihive.core.samples;

import org.wso2.carbon.siddhihive.core.tablecreation.*;
import org.wso2.carbon.siddhihive.core.utils.Constants;

import java.util.ArrayList;
import java.util.List;

public class TestQueryGenerator {

	public static void main(String[] args) {
        // TODO Auto-generated method stub

        List<HiveField> map = new ArrayList<HiveField>();
        map.add(new HiveField("col1", Constants.H_STRING));
        map.add(new HiveField("col2", Constants.H_INT));
        map.add(new HiveField("col3", Constants.H_DOUBLE));
        map.add(new HiveField("col4", Constants.H_INT));

        TableCreatorBase a = new CSVTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        a = new SQLTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        a = new CassandraTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        map.clear();
	}
}
