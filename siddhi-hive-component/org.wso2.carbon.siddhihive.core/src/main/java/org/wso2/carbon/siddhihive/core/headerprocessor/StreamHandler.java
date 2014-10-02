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

package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.siddhi.query.api.query.input.Stream;

import java.util.Map;

/**
 * Interface to define Stream Handling operations
 */
public interface StreamHandler {

	/**
	 * Each streams are processed to generate hive query based on their unique properties
	 * @param stream                Siddhi Stream
	 * @param streamDefinitions     Corresponding stream definition
	 * @return                      Map with corresponding Hive parts. This will be later assembled
	 */
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions);


}
