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

import org.apache.log4j.Logger;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.stream.manager.core.exception.EventStreamConfigurationException;
import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.internal.ds.SiddhiHiveValueHolder;
import org.wso2.carbon.siddhihive.core.internal.exception.SiddhiHiveStreamDefinitionException;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.CassandraTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.TableCreatorBase;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.SiddhiHiveToolBoxCreator;
import org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
  Class to manage query conversion in higher level. Will call appropriate handlers. Will also contain initial data needed for the conversion.
 */

public class SiddhiHiveManager {


    private static final Logger log = Logger.getLogger(SiddhiHiveManager.class);
    private Map<String, StreamDefinitionExt> streamDefinitionMap; //contains stream definition

	public SiddhiHiveManager() {
	    streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
	    Context context = new Context();
	    StateManager.setContext(context);

    }

    public Map<String, StreamDefinitionExt> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(Map<String, StreamDefinitionExt> streamDefinitionMap) {
        this.streamDefinitionMap = streamDefinitionMap;
    }

    public void setStreamDefinition(String streamDefinitionID, StreamDefinitionExt streamDefinition) {
        streamDefinitionMap.put(streamDefinitionID, streamDefinition);
    }

    public void setStreamDefinition(List<StreamDefinitionExt> streamDefinitionList) {
        for (StreamDefinitionExt definition : streamDefinitionList) {
            streamDefinitionMap.put(definition.getStreamDefinition().getStreamId(), definition);
        }
    }

    public void setSiddhiStreamDefinition(List<StreamDefinition> streamDefinitionList) {

        for (StreamDefinition definition : streamDefinitionList) {
            int i = 0;
            for (Map.Entry<String, StreamDefinitionExt> entry : streamDefinitionMap.entrySet()) {
                i++;
                if ((!(definition.getStreamId().equals(entry.getKey()))) && (i < streamDefinitionMap.entrySet().size())) {
                    continue;

                } else if (definition.getStreamId().equals(entry.getKey())) {
                    break;
                }
                StreamDefinitionExt streamDefinition = new StreamDefinitionExt(definition.getStreamId(), definition);
                this.setStreamDefinition(streamDefinition.getFullQualifiedStreamID(), streamDefinition);
            }
        }
    }

    public StreamDefinitionExt getStreamDefinition(String streamId) {
        return streamDefinitionMap.get(streamId);
    }

	/**
	 * This is the place where the Query object of siddhi being passed to the component.
	 * This Query filter object will eventualy turn in to Hive Script
	 * @param query                      Query Object
	 * @return                     Final Hive script
	 */
    public String getQuery(Query query) {

        Boolean incrementalEnabled = false;
        String hiveQuery;
        Context context = StateManager.getContext();

        context.setProcessingLevel(org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel.INPUT_STREAM);
        StateManager.setContext(context);
        HeaderHandler headerHandler = new HeaderHandler(this);
        Map<String, String> headerMap = headerHandler.process(query.getInputStream(), this.getStreamDefinitionMap());


        context.setProcessingLevel(ProcessingLevel.SELECTOR);
        StateManager.setContext(context);
        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor();
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        context.setProcessingLevel(org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel.OUTPUT_STREAM);
        StateManager.setContext(context);
        OutStream outStream = query.getOutputStream();

	    //From stream definition, hive tables re going to get created.
        StreamDefinitionExt outStreamDefinition = getStreamDefinition(outStream.getStreamId());
        TableCreatorBase tableCreator = new CSVTableCreator();
        tableCreator.setQuery(outStreamDefinition);
        String outputInsertQuery = tableCreator.getInsertQuery();
        String outputCreate = tableCreator.getQuery();

        Stream inStream = query.getInputStream();
        List<String> lstIDs = inStream.getStreamIds();
        StreamDefinitionExt inStreamDef;

        String[] arrCreate = new String[lstIDs.size()];
        int i = 0;
        for (String s : lstIDs) {
            inStreamDef = getStreamDefinition(s);
            tableCreator = new CassandraTableCreator();
            tableCreator.setQuery(inStreamDef);
            arrCreate[i++] = tableCreator.getQuery();
        }

        String inputCreate = "";
	    for (String anArrCreate : arrCreate) {
		    inputCreate += anArrCreate;
		    inputCreate += "\n";
	    }

	    //Obtains the from caluse
        String fromClause = headerMap.get(Constants.FROM_CLAUSE);
        if (fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_WIND_FROM_QUERY);
        if(fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);

        if (fromClause == null)
            fromClause = headerMap.get(Constants.JOIN_CLAUSE);

        String initializationScript = headerMap.get(Constants.INITALIZATION_SCRIPT);

        if (initializationScript == null)
            initializationScript = " ";

	    //Select query
        String selectQuery = "SELECT \'Dummy Key\', " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
        String groupByQuery = concurrentSelectorMap.get(Constants.GROUP_BY_QUERY);

        if (groupByQuery == null)
            groupByQuery = " ";

        String havingQuery = concurrentSelectorMap.get(Constants.HAVING_QUERY);

        if (havingQuery == null)
            havingQuery = " ";

        String whereClause = headerMap.get(Constants.WHERE_CLAUSE);

        if (whereClause == null)
            whereClause = " ";

        String incrementalClause = headerMap.get(Constants.INCREMENTAL_CLAUSE);

        if (incrementalClause == null) {
            incrementalClause = " ";
        } else {
            incrementalEnabled = true;
        }

       // hiveQuery = outputQuery + "\n" + incrementalClause + "\n" + fromClause + "\n " + selectQuery + "\n " + groupByQuery + "\n " + havingQuery + "\n " + whereClause + "\n ";
	    hiveQuery = initializationScript + inputCreate + "\n" + outputCreate + "\n" + "\n" +
	                Constants.INITIALIZATION_STATEMENT + "\n" + incrementalClause + "\n" +
	                outputInsertQuery + "\n" + selectQuery + "\n " + fromClause + "\n " +
	                whereClause + "\n " + groupByQuery + "\n " + havingQuery + "\n " + ";";
	    hiveQuery += "\n" + Constants.EXECUTION_FINALIZER +";";

        List<String> streamDefs = new ArrayList<String>();
        for (Map.Entry entry : streamDefinitionMap.entrySet()) {
            if (isInputStream((StreamDefinitionExt) entry.getValue())) {
	            try {
		            streamDefs.add(getOriginalStreamDefinition((StreamDefinitionExt) entry.getValue()));
	            } catch (SiddhiHiveStreamDefinitionException e) {
		           log.error("Obtianing OriginalStreamDefinition failed",e);
	            }
            }
        }
        SiddhiHiveToolBoxCreator siddhiHiveToolBoxCreator = new SiddhiHiveToolBoxCreator(streamDefs, hiveQuery);
	    Long freq = getSlidingFreq(headerMap.get(Constants.TIME_WINDOW_FREQUENCY),
	                               headerMap.get(Constants.TIME_BATCH_WINDOW_FREQUENCY),
	                               headerMap.get(Constants.LENGTH_WINDOW_FREQUENCY),
	                               headerMap.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY));
	    siddhiHiveToolBoxCreator.createToolBox(incrementalEnabled, freq);
        context.reset();
        StateManager.setContext(context);

        return hiveQuery;

    }

	/**
	 * This methods determine the slidign frequencies of both Time and length slidign windows.
	 *
	 * @param timeWindow               timeWindow
	 * @param timeBatchWindow          timeBatchWindow
	 * @param lengthWindow             lengthWindow
	 * @param lengthBatchWindow        lengthBatchWindow
	 * @return              time used for sliding
	 */
    private Long getSlidingFreq(String timeWindow, String timeBatchWindow, String lengthWindow, String lengthBatchWindow) {
        List<Long> freq = new ArrayList<Long>();
        if (timeWindow != null) {
            freq.add(Long.parseLong(timeWindow));
        }
        if (timeBatchWindow != null) {
            freq.add(Long.parseLong(timeBatchWindow));
        }
        if (lengthBatchWindow != null) {
            freq.add(Long.parseLong(lengthBatchWindow));
        }
        if (lengthWindow != null) {
            freq.add(Long.parseLong(lengthWindow));
        }
        if (freq.size() > 0) {
            Collections.sort(freq);
            return freq.get(0);
        } else {
            return (long) 0;
        }
    }

	private Boolean isInputStream(StreamDefinitionExt streamDefinitionExt) {
	    return !streamDefinitionExt.getFullQualifiedStreamID().
	                               equals(streamDefinitionExt.getStreamDefinition().getStreamId());
    }

    private String getOriginalStreamDefinition(StreamDefinitionExt streamDefinitionExt)
		    throws SiddhiHiveStreamDefinitionException {
        int tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
        String streamId = streamDefinitionExt.getFullQualifiedStreamID();
        org.wso2.carbon.databridge.commons.StreamDefinition streamDefinition = null;
        try {
            streamDefinition = SiddhiHiveValueHolder.getInstance().getEventStreamService().getStreamDefinition(streamId, tenantId);
        } catch (EventStreamConfigurationException e) {
            log.error("Error in obtaining Stream Definition",e);
	        throw new SiddhiHiveStreamDefinitionException("Error in obtaining Stream Definition", e);
	    }catch (Throwable e) {
	        log.error("Failed to obtain Stream Definition.", e);

        }

        if (streamDefinition != null) {
            return streamDefinition.toString();
        } else {
            log.error("No stream definition found for stream id " + streamId);
            return null;
        }
    }


}
