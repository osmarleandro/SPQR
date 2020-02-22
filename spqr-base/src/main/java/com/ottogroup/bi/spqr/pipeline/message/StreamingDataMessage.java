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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * Data structure used to transport data through a {@link MicroPipeline}
 * @author mnxfst
 * @since Mar 5, 2015
 */
@JsonRootName ( value = "streamingDataSourceMessage" )
public class StreamingDataMessage implements Serializable {

    public static final String CFG_WAIT_STRATEGY_NAME = "waitStrategy.name";
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
}
