package org.wso2.carbon.siddhihive.core.internal;


import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinition;
import org.wso2.carbon.siddhihive.core.SiddhiHiveServiceInterface;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.Map;

public class SiddhiHiveService implements SiddhiHiveServiceInterface {
    private SiddhiHiveManager siddhiHiveManager;
    private SiddhiManager siddhiManager;

    @Override
    public String addExecutionPlan(ExecutionPlan executionPlan) {
        siddhiHiveManager = new SiddhiHiveManager();
        siddhiHiveManager.setStreamDefinitionMap((java.util.concurrent.ConcurrentMap<String, StreamDefinition>) executionPlan.getStreamDefinitionMap());
        configureSiddhiManager(executionPlan.getStreamDefinitionMap());
        String queryID = siddhiManager.addQuery(executionPlan.getQuery());
        siddhiHiveManager.setSiddhiStreamDefinition(siddhiManager.getStreamDefinitions());
        String hiveQueryString = siddhiHiveManager.getQuery(siddhiManager.getQuery(queryID));
        return hiveQueryString;
    }

    private void configureSiddhiManager(Map<String, StreamDefinition> streamDefinitions) {
        siddhiManager = new SiddhiManager();
        for (Map.Entry<String, StreamDefinition> entry : streamDefinitions.entrySet()) {
            siddhiManager.defineStream(entry.getValue().getStreamDefinition());

        }
    }

    /*@Override
    public Boolean addStreamDefinition(StreamDefinition streamDefinition, String fullQualifiedname) {
        return null;
    }*/
}