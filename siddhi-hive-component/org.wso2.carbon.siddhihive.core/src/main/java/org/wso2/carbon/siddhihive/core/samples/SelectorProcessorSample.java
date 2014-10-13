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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Class used to test the simple and complex siddhi queries with hive script.
 */
public class SelectorProcessorSample {

	private static Log log = LogFactory.getLog(SelectorProcessorSample.class);

    public static void main(String[] args) {



        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream StockExchangeStream ( symbol string, price int )");
        siddhiManager.defineStream("define stream FastMovingStockQuotes ( symbol string, price int, averagePrice double )");
        String queryID = siddhiManager.addQuery(" from StockExchangeStream[price >= 20]#window.time(50) " +
                                                " select symbol, avg(price) as avgPrice " +
                " group by symbol having avgPrice > 50 " +
                " insert into StockQuote;");

        Query query = siddhiManager.getQuery(queryID);

        SiddhiHiveManager siddhiHiveManager = new SiddhiHiveManager();


	       /* String queryID2 = siddhiManager.addQuery("from TickEvent[symbol==’IBM’]#window.length(2000) join"+
                            "NewsEvent#window.time(500) select * insert into JoinStream)";
*/
        String queryID3 = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"IBM\"]#window.length(1)" +
        "select symbol,price, avg(price) as averagePrice \n" +
                "group by symbol, price  \n" +
                "having ((price > averagePrice*1.02) and (averagePrice*0.98 > price ))\n" +
                "insert into FastMovingStockQuotes");

        log.info("+++++++++++++++++++++++++++");
//        String queryID = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"IBM\"]#window.lengthBatch(1)\n" +
//                "select symbol,price, avg(price) as averagePrice \n" +
//                "group by symbol\n" +
//                "having ((price > averagePrice*1.02) and ( (averagePrice*0.98 > price) or (averagePrice*0.98 < price) ))\n" +
//                "insert into FastMovingStockQuotes;");


/*

            String queryID2 = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"Apple\"]#window.time(6000) \n" +
                    "select symbol,price, avg(price) as averagePrice  \n" +
                    "group by symbol \n" +
                    " having ((price > averagePrice*1.02) or (averagePrice*0.98 > price ))\n" +
                    "insert into FastMovingStockQuotes;");
*/

//           String queryID = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"Apple\"]#window.time(6000) \n" +
//                "join FastMovingStockQuotes#window.lengthBatch(15)  \n" +
//                " on StockExchangeStream.symbol == FastMovingStockQuotes.symbol  select *\n" +
//                "insert into JoinStream;");


            /**
             * from AllStockQuotes#window.time(600000)
             select symbol,price, avg(price) as averagePrice
             group by symbol
             having ((price > averagePrice*1.02) or (averagePrice*0.98 > price ))
             insert into FastMovingStockQuotes;
             */


        Query query2 = siddhiManager.getQuery(queryID);


        List<StreamDefinition> streamDefinitionList = siddhiManager.getStreamDefinitions();
        List<StreamDefinitionExt> streamDefinitionExtList = new ArrayList<StreamDefinitionExt>() ;

        for (int i = 0; i < streamDefinitionList.size(); ++i) {

            StreamDefinition streamDefinition = streamDefinitionList.get(i);
            StreamDefinitionExt streamDefinitionExt = new StreamDefinitionExt(streamDefinition.getStreamId(), streamDefinition);

            streamDefinitionExtList.add(streamDefinitionExt);
           // siddhiHiveManager.setStreamDefinition(streamDefinition.getStreamId(), streamDefinition);
        }

        siddhiHiveManager.setStreamDefinition(streamDefinitionExtList);

        String hiveQuery = siddhiHiveManager.getQuery(query);
	        log.info(hiveQuery);
//
//        try {
//            Thread.sleep(1000);
//        }catch (Exception ex){
//
//        }

       }
}
