/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ottogroup.bi.spqr.operator.json.aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ottogroup.bi.spqr.exception.ComponentInitializationFailedException;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.operator.json.JsonContentType;
import com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponentType;
import com.ottogroup.bi.spqr.pipeline.component.annotation.SPQRComponent;
import com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator;
import com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperatorWaitStrategy;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;

/**
 * Aggregates content of JSON documents provided
 * @author mnxfst
 * @since Mar 17, 2015
 */
@SPQRComponent(type=MicroPipelineComponentType.DELAYED_RESPONSE_OPERATOR, name="jsonContentAggregator", version="0.0.1", description="Aggregates arbitrary JSON content")
public class JsonContentAggregator extends SuperclassExtracted implements DelayedResponseOperator {

	/** our faithful logging facility .... ;-) */
	public static final Logger logger = Logger.getLogger(JsonContentAggregator.class);
	
	////////////////////////////////////////////////////////////////////////
	// available settings
	/** if provided the pipeline identifier is added to output document */
	public static final String CFG_PIPELINE_ID = "pipelineId";
	/** type assigned to each output document */
	public static final String CFG_DOCUMENT_TYPE = "documentType";
	/** store and forward raw data - default: true */
	public static final String CFG_FORWARD_RAW_DATA = "forwardRawData";
	/** prefix to all field settings - required: field.1.name, field.1.path and field.1.type (settings must use continuous enumeration starting with value 1 */
	public static final String CFG_FIELD_PREFIX = "field.";
	//
	////////////////////////////////////////////////////////////////////////

	/** maps inbound strings into object representations and json strings vice versa */
	public final ObjectMapper jsonMapper = new ObjectMapper();
	/** identifier as assigned to surrounding pipeline */
	public String pipelineId = null;
	/** document identifier added to each output message */
	public String documentType = null;
	/** overall number of messages processed */ 
	public long messageCount = 0;
	/** number of messages processed since last result collection */	
	public long messagesSinceLastResult = 0;
	/** store and forward raw data - default: true */
	public boolean storeForwardRawData = true;
	/** fields considered to be relevant mapped to aggregator that must be applied to values - none = data is added to raw output only */
	public List<JsonContentAggregatorFieldSetting> fields = new ArrayList<>();
	/** result document - reset after specified duration */
	public JsonContentAggregatorResult resultDocument = null;

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent#initialize(java.util.Properties)
	 */
	public void initialize(Properties properties) throws RequiredInputMissingException,	ComponentInitializationFailedException {
		
		/////////////////////////////////////////////////////////////////////////////////////
		// assign and validate properties
		if(StringUtils.isBlank(this.id))
			throw new RequiredInputMissingException("Missing required component identifier");
		
		this.pipelineId = StringUtils.trim(properties.getProperty(CFG_PIPELINE_ID));		
		this.documentType = StringUtils.trim(properties.getProperty(CFG_DOCUMENT_TYPE));
		if(StringUtils.equalsIgnoreCase(properties.getProperty(CFG_FORWARD_RAW_DATA), "false"))
			this.storeForwardRawData = false;
		
		
		for(int i = 1; i < Integer.MAX_VALUE; i++) {
			String name = properties.getProperty(CFG_FIELD_PREFIX + i + ".name");
			if(StringUtils.isBlank(name))
				break;
			
			String path = properties.getProperty(CFG_FIELD_PREFIX + i + ".path");
			String valueType = properties.getProperty(CFG_FIELD_PREFIX + i + ".type");
			
			this.fields.add(new JsonContentAggregatorFieldSetting(name, path.split("\\."), StringUtils.equalsIgnoreCase("STRING", valueType) ? JsonContentType.STRING : JsonContentType.NUMERICAL));
		}
		/////////////////////////////////////////////////////////////////////////////////////
		
		if(logger.isDebugEnabled())
			logger.debug("json content aggregator [id="+id+"] initialized");
		
	}

	private void extracted(Properties properties) {
		this.pipelineId = StringUtils.trim(properties.getProperty(CFG_PIPELINE_ID));		
		this.documentType = StringUtils.trim(properties.getProperty(CFG_DOCUMENT_TYPE));
		if(StringUtils.equalsIgnoreCase(properties.getProperty(CFG_FORWARD_RAW_DATA), "false"))
			this.storeForwardRawData = false;
		

		for(int i = 1; i < Integer.MAX_VALUE; i++) {
			String name = properties.getProperty(CFG_FIELD_PREFIX + i + ".name");
			if(StringUtils.isBlank(name))
				break;
			
			String path = properties.getProperty(CFG_FIELD_PREFIX + i + ".path");
			String valueType = properties.getProperty(CFG_FIELD_PREFIX + i + ".type");
			
			this.fields.add(new JsonContentAggregatorFieldSetting(name, path.split("\\."), StringUtils.equalsIgnoreCase("STRING", valueType) ? JsonContentType.STRING : JsonContentType.NUMERICAL));
		}
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#getResult()
	 */
	public StreamingDataMessage[] getResult() {
		this.messagesSinceLastResult = 0;
		
		StreamingDataMessage message = null;
		try {			
			message = new StreamingDataMessage(jsonMapper.writeValueAsBytes(this.resultDocument), System.currentTimeMillis());
		} catch (JsonProcessingException e) {
			logger.error("Failed to convert result document into JSON");
		}
		this.resultDocument = new JsonContentAggregatorResult(this.pipelineId, this.documentType);
		return new StreamingDataMessage[]{message};
	}

	/**
	 * Walks along the path provided and reads out the leaf value which is returned as long value
	 * @param jsonNode
	 * @param fieldPath
	 * @return
	 */
	public long getNumericalFieldValue(final JsonNode jsonNode, final String[] fieldPath) {

		int fieldAccessStep = 0;
		JsonNode contentNode = jsonNode;
		while(fieldAccessStep < fieldPath.length) {
			contentNode = contentNode.get(fieldPath[fieldAccessStep]);
			fieldAccessStep++;
		}	

		return contentNode.asLong();
	}
	
	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#setWaitStrategy(com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperatorWaitStrategy)
	 */
	public void setWaitStrategy(DelayedResponseOperatorWaitStrategy waitStrategy) {
		// do nothing as the reference is not required
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent#shutdown()
	 */
	public boolean shutdown() {
		return true;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#getNumberOfMessagesSinceLastResult()
	 */
	public long getNumberOfMessagesSinceLastResult() {
		return this.messagesSinceLastResult;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent#getType()
	 */
	public MicroPipelineComponentType getType() {
		return MicroPipelineComponentType.DELAYED_RESPONSE_OPERATOR;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.Operator#getTotalNumOfMessages()
	 */
	@Override
	public long getTotalNumOfMessages() {
		return this.messageCount;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent#setId(java.lang.String)
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent#getId()
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#onMessage(com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage)
	 * @deprecated Use {@link com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage#onMessage(com.ottogroup.bi.spqr.operator.json.aggregator.JsonContentAggregator)} instead
	 */
	public void onMessage(StreamingDataMessage message) {
		message.onMessage(this);
	}

}
