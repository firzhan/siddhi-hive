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

import org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.SelectorProcessingLevel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to hold context information needed in processing  queries.
 */
public class Context {


    private Map<String, String> cachedValuesMap= null; //parent refernce
    private Map<String, String> selectionAttributeRenameMap = null;
    private Map<String, String> referenceIDAliasMap = null;

    private int subQueryCounter = 0;

    //enums
    private ProcessingLevel processingLevel;
	private SelectorProcessingLevel selectorProcessingLevel;

	private int timeStampCounter = 0;
    private int limitCounter = 0;


    public Context(){

        cachedValuesMap = new ConcurrentHashMap<String, String>();
        selectionAttributeRenameMap = new ConcurrentHashMap<String, String>();
        referenceIDAliasMap = new ConcurrentHashMap<String, String>();

        processingLevel = ProcessingLevel.NONE;
	    selectorProcessingLevel = SelectorProcessingLevel.NONE;

    }

	/**
	 * Sub query Identifiers are used as a variable to assign the entire query in a complicated hive query.
	 *
	 * @return returns an auto increment variable to be assigned to the entire sub query. later on
	 *          on another place these variables can be used to represent that particular query.
	 */
    public String generateSubQueryIdentifier(){
        return  "subq" + String.valueOf(++subQueryCounter);
    }

    public String generatePreviousSubQueryIdentifier(){
        return "subq" + String.valueOf(subQueryCounter - 1);
    }

    public String getSelectionAttributeRename(String rename) {
        return this.selectionAttributeRenameMap.get(rename);
    }

	/**
	 * In siddhi query selection related sql syntax are used with AS keyword. Hive doesn't support that functionality.
	 * But later on the siddhi query, it refers the syntax with the new name ( Value used after AS  ).
	 * In order to track them we are using this map.
	 *
	 * @param rename          Value occurs after AS
	 * @param selectionString Siddhi string denoted by  AS keyword
	 */
    public void addSelectionAttributeRename(String rename, String selectionString) {
        this.selectionAttributeRenameMap.put(rename, selectionString);
    }

    public ProcessingLevel getProcessingLevel() {
        return processingLevel;
    }

    public void setProcessingLevel(ProcessingLevel processingLevel) {
	    this.processingLevel = processingLevel;
    }


    public SelectorProcessingLevel getSelectorProcessingLevel() {
        return selectorProcessingLevel;
    }

    public void setSelectorProcessingLevel(SelectorProcessingLevel selectorProcessingLevel) {
        this.selectorProcessingLevel = selectorProcessingLevel;
    }

    public String getReferenceIDAlias(String referenceID) {
        return referenceIDAliasMap.get(referenceID);
    }

    public void setReferenceIDAlias(String referenceID, String alias) {
        this.referenceIDAliasMap.put(referenceID, alias);
    }

    public int generateTimeStampCounter(boolean generateNew){

        if(!generateNew)
            return timeStampCounter;

        return ++timeStampCounter;
    }

    public void reset(){
        timeStampCounter = 0;
        subQueryCounter = 0;
        limitCounter = 0;

        cachedValuesMap.clear();
        selectionAttributeRenameMap.clear();
        referenceIDAliasMap.clear();

        processingLevel = ProcessingLevel.NONE;
	    selectorProcessingLevel = SelectorProcessingLevel.NONE;

    }


    public int generateLimitCounter(boolean generateNew){
        if(!generateNew)
            return limitCounter;
        return ++limitCounter;
    }
}
