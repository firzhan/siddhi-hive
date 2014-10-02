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


import org.apache.log4j.Logger;
import org.osgi.service.component.ComponentContext;
import org.wso2.carbon.event.stream.manager.core.EventStreamService;
import org.wso2.carbon.siddhihive.core.SiddhiHiveServiceInterface;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

/**
 * @scr.component name="siddhiHive.component" immediate="true"
 * @scr.reference name="eventStreamManager.service"
 * interface="org.wso2.carbon.event.stream.manager.core.EventStreamService" cardinality="1..1"
 * policy="dynamic" bind="setEventStreamManagerService" unbind="unsetEventStreamManagerService"
 */
public class SiddhiHiveDS {
    private static final Logger log = Logger.getLogger(SiddhiHiveDS.class);

    /**
     * Exposing SiddhiHive OSGi service.
     *
     * @param context
     */

    protected void activate(ComponentContext context) {

        try {
            SiddhiHiveService siddhiHiveService = new SiddhiHiveService();
            SiddhiHiveValueHolder.getInstance().registerSiddhiHiveService(siddhiHiveService);
            context.getBundleContext().registerService(SiddhiHiveServiceInterface.class.getName(),
                    siddhiHiveService, null);
            log.info("Successfully deployed the Siddhi-Hive conversion Service");
        } catch (Throwable e) {
            log.error("Can not create the Siddhi-Hive conversion Service ", e);
        }
    }

    protected void deactivate(ComponentContext context) {
        //context.getBundleContext().ungetService();
    }

    protected void setEventStreamManagerService(EventStreamService eventStreamService) {
        SiddhiHiveValueHolder.getInstance().setEventStreamService(eventStreamService);
    }

    protected void unsetEventStreamManagerService(EventStreamService eventStreamService) {
        SiddhiHiveValueHolder.getInstance().unsetEventStreamService();
    }
}
