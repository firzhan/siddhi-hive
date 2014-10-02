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

package org.wso2.carbon.siddhihive.core.configurations;


import java.util.Map;

/**
 * Execution Plans form the data bridge are used to obtain the fully qualified stream ID for each stream definition.
 * Stream definition is stored in StreamDefinitionExt class
 */
public class ExecutionPlan {
    private String query;
    private Map<String, StreamDefinitionExt> streamDefinitionMap;

    public ExecutionPlan(String query, Map<String, StreamDefinitionExt> streamDefinitionMap) {
        this.query = query;
        this.streamDefinitionMap = streamDefinitionMap;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, StreamDefinitionExt> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

}
