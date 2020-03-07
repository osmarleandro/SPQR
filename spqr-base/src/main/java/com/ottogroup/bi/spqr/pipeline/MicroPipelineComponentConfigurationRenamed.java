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
package com.ottogroup.bi.spqr.pipeline;

import java.io.Serializable;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponent;
import com.ottogroup.bi.spqr.pipeline.component.MicroPipelineComponentType;

/**
 * Configuration required for setting up a {@link MicroPipelineComponent} instance
 * @author mnxfst
 * @since Mar 6, 2015
 */
@JsonRootName(value="componentConfiguration")
public class MicroPipelineComponentConfigurationRenamed implements Serializable {

	private static final long serialVersionUID = 3896521083804105532L;

	/** component identifier */
	@JsonProperty(value="id", required=true)
	private String id = null;
	/** component type: SOURCE, OPERATOR, ... */
	@JsonProperty(value="type", required=true)
	private MicroPipelineComponentType type = null;
	/** component class */
	@JsonProperty(value="name", required=true)
	private String name = null;
	/** component version */
	@JsonProperty(value="version", required=true)
	private String version = null;
	/** settings required for component configuration */
	@JsonProperty(value="settings", required=true)
	private Properties settings = new Properties();
	/** identifier of queues to consume content from */
	@JsonProperty(value="fromQueue", required=true)
	private String fromQueue = null;
	/** identifier of queues to produce content to */
	@JsonProperty(value="toQueue", required=true)
	private String toQueue = null;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public MicroPipelineComponentType getType() {
		return type;
	}
	public void setType(MicroPipelineComponentType type) {
		this.type = type;
	}
	public Properties getSettings() {
		return settings;
	}
	public void setSettings(Properties settings) {
		this.settings = settings;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getFromQueue() {
		return fromQueue;
	}
	public void setFromQueue(String fromQueue) {
		this.fromQueue = fromQueue;
	}
	public String getToQueue() {
		return toQueue;
	}
	public void setToQueue(String toQueue) {
		this.toQueue = toQueue;
	}
}
