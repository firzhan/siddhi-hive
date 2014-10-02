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

import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class SampleHelper {

    public ExecutionPlan getExecutionPlan(String query, List<String> streamDefList, List<String> fullQualifiedName) {
        if (streamDefList.size() == fullQualifiedName.size()) {
            SiddhiManager siddhiManager = new SiddhiManager();
            for (String definition : streamDefList) {
                siddhiManager.defineStream(definition);
            }
            List<StreamDefinition> siddhiDef = siddhiManager.getStreamDefinitions();
            Map<String, StreamDefinitionExt> siddhiHiveDef = new ConcurrentHashMap<String, StreamDefinitionExt>();
            for (int i = 0; i < siddhiDef.size(); i++) {
                siddhiHiveDef.put(siddhiDef.get(i).getStreamId(), new StreamDefinitionExt(fullQualifiedName.get(i), siddhiDef.get(i)));
            }
            ExecutionPlan executionPlan = new ExecutionPlan(query, siddhiHiveDef);
            return executionPlan;
        }
        return null;
    }
}
