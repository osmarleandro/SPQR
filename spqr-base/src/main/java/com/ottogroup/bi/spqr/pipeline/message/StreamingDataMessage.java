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
package com.ottogroup.bi.spqr.pipeline.message;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.JsonNode;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.operator.json.JsonContentType;
import com.ottogroup.bi.spqr.operator.json.aggregator.JsonContentAggregator;
import com.ottogroup.bi.spqr.operator.json.aggregator.JsonContentAggregatorFieldSetting;
import com.ottogroup.bi.spqr.operator.json.aggregator.JsonContentAggregatorResult;

/**
 * Data structure used to transport data through a {@link MicroPipeline}
 * @author mnxfst
 * @since Mar 5, 2015
 */
@JsonRootName ( value = "streamingDataSourceMessage" )
public class StreamingDataMessage implements Serializable {

	private static final long serialVersionUID = 1280809436656124315L;
	
	/** message body a.k.a its content */
	@JsonProperty ( value = "body", required = true )
	private byte[] body = null;
	
	/** time the message entered the system */
	@JsonProperty ( value = "timestamp", required = true )
	private long timestamp = 0;
	
	/**
	 * Default constructor
	 */
	public StreamingDataMessage() {		
	}
	
	/**
	 * Initializes the message using the provided input
	 * @param body
	 * @param timestamp
	 */
	public StreamingDataMessage(final byte[] body, final long timestamp) {
		this.body = body;
		this.timestamp = timestamp;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @param jsonContentAggregator TODO
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#onMessage(com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage)
	 */
	public void onMessage(JsonContentAggregator jsonContentAggregator) {		
		jsonContentAggregator.messageCount++;
		jsonContentAggregator.messagesSinceLastResult++;
		
		
		// do nothing if either the event or the body is empty
		if(this == null || getBody() == null || getBody().length < 1)
			return;
		
		JsonNode jsonNode = null;
		try {
			jsonNode = jsonContentAggregator.jsonMapper.readTree(getBody());
		} catch(IOException e) {
			JsonContentAggregator.logger.error("Failed to read message body to json node. Ignoring message. Error: " + e.getMessage());
		}
		
		// return null in case the message could not be parsed into 
		// an object representation - the underlying processor does
		// not forward any NULL messages
		if(jsonNode == null)
			return;
		
		// initialize the result document if not already done
		if(jsonContentAggregator.resultDocument == null)
			jsonContentAggregator.resultDocument = new JsonContentAggregatorResult(jsonContentAggregator.pipelineId, jsonContentAggregator.documentType);
		
		Map<String, Object> rawData = new HashMap<>();
		// step through fields considered to be relevant, extract values and apply aggregation function
		for(final JsonContentAggregatorFieldSetting fieldSettings : jsonContentAggregator.fields) {
			
			// switch between string and numerical field values
			// string values may be counted only
			// numerical field values must be summed, min and max computed AND counted 
			
			// string values may be counted only
			if(fieldSettings.getValueType() == JsonContentType.STRING) {
	
				try {
					// read value into string representation and add it to raw data dump
					String value = jsonContentAggregator.getTextFieldValue(jsonNode, fieldSettings.getPath());
					if(jsonContentAggregator.storeForwardRawData)
						rawData.put(fieldSettings.getField(), value);
					
					// count occurrences of value
					try {
						jsonContentAggregator.resultDocument.incAggregatedValue(fieldSettings.getField(), value, 1);
					} catch (RequiredInputMissingException e) {
						JsonContentAggregator.logger.error("Field '"+fieldSettings.getField()+"' not found in event. Ignoring value. Error: " +e.getMessage());
					}
				} catch(Exception e) {
				}
			} else if(fieldSettings.getValueType() == JsonContentType.NUMERICAL) {			
				
				try {
					// read value into numerical representation and add it to raw data map
					long value = jsonContentAggregator.getNumericalFieldValue(jsonNode, fieldSettings.getPath());
					if(jsonContentAggregator.storeForwardRawData)
						rawData.put(fieldSettings.getField(), value);
					
					// compute min, max and sum and add these values to result document
					try {
						jsonContentAggregator.resultDocument.evalMinAggregatedValue(fieldSettings.getField(), "min", value);
						jsonContentAggregator.resultDocument.evalMaxAggregatedValue(fieldSettings.getField(), "max", value);
						jsonContentAggregator.resultDocument.incAggregatedValue(fieldSettings.getField(), "sum", value);
					} catch(RequiredInputMissingException e) {
						JsonContentAggregator.logger.error("Field '"+fieldSettings.getField()+"' not found in event. Ignoring value. Error: " +e.getMessage());
					}				
					
				} catch(Exception e) {
				}
			}			
		}
		
		// add raw data to document
		if(jsonContentAggregator.storeForwardRawData)
			jsonContentAggregator.resultDocument.addRawData(rawData);
	}
}
