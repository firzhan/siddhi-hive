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
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

import java.util.ArrayList;
import java.util.List;


public class Sample01 {


    private static String streamdef1 = "define stream StockExchangeStream1 ( symbol string, price int )";
    private static String streamdef2 = "define stream StockExchangeStream2 ( symbol string, quantity int )";
    //private static String streamdef3 = "define stream StockQuote1 ( symbol string, avgPrice double, avgQnt int )";
    private static String query1 = " from StockExchangeStream1[price >= 20]#window.time(50) as t join StockExchangeStream2#window.time(50) as n on t.symbol==n.symbol" +
            " select t.symbol, avg(t.price) as avgPrice, avg(n.quantity) as avgQnt" +
            " group by symbol having avgPrice > 50 " +
            " insert into StockQuote1;";
    private static String fullName1 = "org_wso2_carbon_kpi_publisher1";
    private static String fullName2 = "org_wso2_carbon_kpi_publisher2";

    private static List<String> defList = new ArrayList<String>();
    private static List<String> nameList = new ArrayList<String>();

    public static void main(String[] args) {
        defList.add(streamdef1);
        defList.add(streamdef2);

        nameList.add(fullName1);
        nameList.add(fullName2);

        SampleHelper sampleHelper = new SampleHelper();
        ExecutionPlan executionPlan = sampleHelper.getExecutionPlan(query1, defList, nameList);
        SiddhiHiveService siddhiHiveService = new SiddhiHiveService();
        String result = siddhiHiveService.addExecutionPlan(executionPlan);
        System.out.println(result);
    }
}
