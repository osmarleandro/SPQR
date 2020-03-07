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
package com.ottogroup.bi.spqr.pipeline.component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueConfiguration;

/**
 * Configuration required for setting up a {@link MicroPipeline} instance
 * @author mnxfst
 * @since Mar 6, 2015
 */
@JsonRootName(value="microPipelineConfiguration")
public class MicroPipelineConfigurationRenamed implements Serializable {

	private static final long serialVersionUID = 3063209038310841880L;

	/** pipeline identifier which must be unique within the cluster */
	@JsonProperty(value="id", required=true)
	private String id = null;
	/** configuration of queues to be used by components */
	@JsonProperty(value="queues", required=true) 
	private List<StreamingMessageQueueConfiguration> queues = new ArrayList<>();
	/** component configurations */
	@JsonProperty(value="components", required=true)
	private List<MicroPipelineComponentConfiguration> components = new ArrayList<>();
	/** timer for collecting components stats */
	@JsonProperty(value="statsCollectionTimer", required=true)
	private long statsCollectionTimer = 1000;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getStatsCollectionTimer() {
		return statsCollectionTimer;
	}
	public void setStatsCollectionTimer(long statsCollectionTimer) {
		this.statsCollectionTimer = statsCollectionTimer;
	}
	public List<StreamingMessageQueueConfiguration> getQueues() {
		return queues;
	}
	public void setQueues(List<StreamingMessageQueueConfiguration> queues) {
		this.queues = queues;
	}
	public List<MicroPipelineComponentConfiguration> getComponents() {
		return components;
	}
	public void setComponents(List<MicroPipelineComponentConfiguration> components) {
		this.components = components;
	}
	
	
	
}
