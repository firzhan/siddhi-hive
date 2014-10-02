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

package org.wso2.carbon.siddhihive.core.internal.ds;

import org.wso2.carbon.event.stream.manager.core.EventStreamService;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

public class SiddhiHiveValueHolder {
    private EventStreamService eventStreamService;
    private static SiddhiHiveValueHolder siddhiHiveValueHolder;
    private SiddhiHiveService siddhiHiveService;

    private SiddhiHiveValueHolder() {
        //Do nothing
    }

    public static SiddhiHiveValueHolder getInstance() {
        if (siddhiHiveValueHolder == null) {
            siddhiHiveValueHolder = new SiddhiHiveValueHolder();
        }
        return siddhiHiveValueHolder;
    }

    public EventStreamService getEventStreamService() {
        return eventStreamService;
    }

    public void setEventStreamService(EventStreamService eventStreamService) {
        this.eventStreamService = eventStreamService;
    }

    public void unsetEventStreamService() {
        this.eventStreamService = null;
    }

    public void registerSiddhiHiveService(SiddhiHiveService siddhiHiveService) {
        this.siddhiHiveService = siddhiHiveService;
    }

    public SiddhiHiveService getSiddhiHiveService() {
        return siddhiHiveService;
    }
}
