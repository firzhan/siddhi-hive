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

package org.wso2.carbon.siddhihive.core.internal;

import org.wso2.carbon.siddhihive.core.SiddhiHiveServiceInterface;
import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.query.Query;

import java.util.Map;

/**
 * Interface to interact with data-bridge to obtain the execution plan information
 */

public class SiddhiHiveService implements SiddhiHiveServiceInterface {
     private SiddhiManager siddhiManager;

    @Override
    public String addExecutionPlan(ExecutionPlan executionPlan) {
	    SiddhiHiveManager siddhiHiveManager = new SiddhiHiveManager();
        siddhiHiveManager.setStreamDefinitionMap(executionPlan.getStreamDefinitionMap());
        configureSiddhiManager(executionPlan.getStreamDefinitionMap());
        String queryID = siddhiManager.addQuery(executionPlan.getQuery());
        siddhiHiveManager.setSiddhiStreamDefinition(siddhiManager.getStreamDefinitions());
        Query query = siddhiManager.getQuery(queryID);
        return siddhiHiveManager.getQuery(query);
    }

    private void configureSiddhiManager(Map<String, StreamDefinitionExt> streamDefinitions) {
        siddhiManager = new SiddhiManager();
        for (Map.Entry<String, StreamDefinitionExt> entry : streamDefinitions.entrySet()) {
            siddhiManager.defineStream(entry.getValue().getStreamDefinition());
        }
    }
}
