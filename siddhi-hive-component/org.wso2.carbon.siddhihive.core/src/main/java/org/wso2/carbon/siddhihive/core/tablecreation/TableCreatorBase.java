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

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.utils.Conversions;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;

public abstract class TableCreatorBase {
    protected String sInsertQuery = "";
    protected String sHiveColumns = "";

    protected String sDBName;
    protected String sFullStreamID;
    protected List<HiveField> listColumns;
	
	//**********************************************************************************************
	public TableCreatorBase() {
		sDBName = "";
        sFullStreamID = "";
        listColumns = new ArrayList<HiveField>();
	}

    //**********************************************************************************************
    public void setQuery(StreamDefinitionExt streamDef) {
        sFullStreamID = streamDef.getFullQualifiedStreamID();
        StreamDefinition def = streamDef.getStreamDefinition();
        sDBName = def.getStreamId();
        List<Attribute> attributeList = def.getAttributeList();
        sDBName = def.getStreamId();
        listColumns = new ArrayList<HiveField>();
        Attribute attribute = null;
        for(int i = 0; i < attributeList.size(); i++) {
            attribute = attributeList.get(i);
            listColumns.add(new HiveField(attribute.getName(), Conversions.siddhiToHiveType(attribute.getType())));
        }
    }

    //**********************************************************************************************
    public void setQuery(String sDB, List<HiveField> listFields, String sFullQualifiedStreamID) {
        sDBName = sDB;
        listColumns = listFields;
        sFullStreamID = sFullQualifiedStreamID;
    }

    //**********************************************************************************************
    public String getInsertQuery() {
        sInsertQuery = "";

        if (sDBName.length() <= 0)
            return null;

        sInsertQuery = "INSERT OVERWRITE TABLE " + sDBName + " ";

        return sInsertQuery;
    }

    //**********************************************************************************************
    public abstract String getQuery();

    //**********************************************************************************************
    protected void fillHiveFieldString() {
        sHiveColumns = "primeKey STRING ,  \t";
        sHiveColumns += listColumns.get(0).getFieldName() + " " + listColumns.get(0).getDataType();
        for (int i = 1; i < listColumns.size(); i++) {
            sHiveColumns += (", " + listColumns.get(i).getFieldName() + " " + listColumns.get(i).getDataType());
        }
        sHiveColumns += (", timestamps BIGINT") ;
    }
}