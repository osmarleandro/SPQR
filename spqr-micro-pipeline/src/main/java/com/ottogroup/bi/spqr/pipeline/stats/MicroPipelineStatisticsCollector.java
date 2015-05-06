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
package com.ottogroup.bi.spqr.pipeline.stats;

import org.apache.log4j.Logger;

import com.ottogroup.bi.spqr.pipeline.component.source.SourceRuntimeEnvironment;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueConsumer;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueWaitStrategy;
import com.ottogroup.bi.spqr.pipeline.statistics.MicroPipelineStatistics;

/**
 * Receives {@link MicroPipelineStatistics} generated and emitted by {@link SourceRuntimeEnvironment}
 * @author mnxfst
 * @since Apr 14, 2015
 */
public class MicroPipelineStatisticsCollector implements Runnable {

	/** our faithful logging facility ... ;-) */
	private final static Logger logger = Logger.getLogger(MicroPipelineStatisticsCollector.class);
	
	/** identifier of pipeline the collector is attached to */
	private final String pipelineId;
	/** consumer pointing to queue holding all statistical information generated by components of a single pipeline */
	private final StreamingMessageQueueConsumer queueConsumer;
	/** consumer queue wait strategy */
	private final StreamingMessageQueueWaitStrategy consumerQueueWaitStrategy;
	/** indicates whether the collector is still running or not - used for notifying about shutdown */
	private boolean running = false;

	/**
	 * Initializes the statistics collector using the provided input
	 * @param pipelineId
	 * @param queueConsumer
	 */
	public MicroPipelineStatisticsCollector(final String pipelineId, final StreamingMessageQueueConsumer queueConsumer) {
		this.queueConsumer = queueConsumer;
		this.consumerQueueWaitStrategy = queueConsumer.getWaitStrategy();
		this.pipelineId = pipelineId;
		logger.info("statsCollector[pipelineId="+this.pipelineId+", queue=" + queueConsumer.getQueueId() +", state=initialized]");
	}
	
	/**
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
		this.running = true;
		
		while(this.running) {
			try {
				
				// fetch the next message from the attached queue via the wait strategy 
				final StreamingDataMessage message = this.consumerQueueWaitStrategy.waitFor(this.queueConsumer);
				
				// TODO irgendwo hin damit
			} catch (InterruptedException e) {
				// do nothing - waiting was interrupted				
			}
		}
		
		logger.info("statsCollector[pipelineId="+this.pipelineId+", queue="+queueConsumer.getQueueId() +", state=shutdown}");
	}

}