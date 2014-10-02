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

/**
 *  Siddhi stream definition map doesn't maintain the fully qualified stream ID along with the
 *  Stream definition.
 *
 *  Therefore this class maintains the Stream Definition along with fully qualified stream ID.
 *  This is essential when creating the Hive table.
 */
public class StreamDefinitionExt {
    private String fullQualifiedStreamID;
    private org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition;

    public StreamDefinitionExt(String streamId, org.wso2.siddhi.query.api.definition.StreamDefinition definition) {
        this.fullQualifiedStreamID = streamId;
        this.streamDefinition = definition;
    }

    public String getFullQualifiedStreamID() {
        return fullQualifiedStreamID;
    }

	public org.wso2.siddhi.query.api.definition.StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

}
