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
package com.ottogroup.bi.spqr.pipeline.component.operator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.codahale.metrics.Counter;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueConsumer;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueProducer;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueWaitStrategy;

/**
 * Provides a runtime environment for {@link DelayedResponseOperator} instances. The environment polls
 * messages from the assigned {@link StreamingMessageQueueConsumer} and forwards them for further processing
 * to the {@link DirectResponseOperator}. In case the condition evaluated by the {@link DelayedResponseOperatorWaitStrategy}
 * provided on startup holds, the environment asks the operator to return its {@link DelayedResponseOperator#getResult() results}
 * which are forwarded to the {@link StreamingMessageQueueProducer} (order is preserved as received from operator).
 * @author mnxfst
 * @since Mar 11, 2015
 */
public class DelayedResponseOperatorRuntimeEnvironment implements Runnable, DelayedResponseCollector {

	/** our faithful logging facility ... ;-) */ 
	private static final Logger logger = Logger.getLogger(DelayedResponseOperatorRuntimeEnvironment.class);

	/** identifier of processing node the runtime environment belongs to*/
	private final String processingNodeId;
	/** identifier of pipeline the runtime environment belongs to */
	private final String pipelineId;
	/** identifier of operator assigned to this runtime environment */
	private final String operatorId; 
	/** operator instance executed by this runtime environment */
	private final DelayedResponseOperator delayedResponseOperator;
	/** strategy to apply when waiting for responses */
	private final DelayedResponseOperatorWaitStrategy responseWaitStrategy;
	/** provides read access to assigned source queue */
	private final StreamingMessageQueueConsumer queueConsumer;
	/** provides write access to assigned destination queue */
	private final StreamingMessageQueueProducer queueProducer;	
	/** indicates whether the operator runtime is still running or not */
	private boolean running = false;
	/** executor environment used to run the response wait strategy */
	private final ExecutorService executorService;
	/** local executor service? - must be shut down as well, otherwise the provider must take care of it */
	private boolean localExecutorService = false;
	/** consumer queue wait strategy */
	private final StreamingMessageQueueWaitStrategy consumerQueueWaitStrategy;
	/** destination queue wait strategy */
	private final StreamingMessageQueueWaitStrategy destinationQueueWaitStrategy;
	/** message counter metric */
	private Counter messageCounter = null;


	/**
	 * Initializes the runtime environment using the provied input
	 * @param processingNodeId
	 * @param pipelineId
	 * @param delayedResponseOperator
	 * @param responseWaitStrategy
	 * @param queueConsumer
	 * @param queueProducer
	 * @throws RequiredInputMissingException
	 */
	public DelayedResponseOperatorRuntimeEnvironment(final String processingNodeId, final String pipelineId, final DelayedResponseOperator delayedResponseOperator, final DelayedResponseOperatorWaitStrategy responseWaitStrategy,
			final StreamingMessageQueueConsumer queueConsumer, final StreamingMessageQueueProducer queueProducer) throws RequiredInputMissingException {
		this(processingNodeId, pipelineId, delayedResponseOperator, responseWaitStrategy, queueConsumer, queueProducer, Executors.newCachedThreadPool());
		this.localExecutorService = true;
	}
	
	/**
	 * Initializes the runtime environment using the provided input
	 * @param processingNodeId
	 * @param pipelineId
	 * @param delayedResponseOperator
	 * @param responseWaitStrategy
	 * @param queueConsumer
	 * @param queueProducer
	 * @param executorService
	 * @throws RequiredInputMissingException
	 */
	public DelayedResponseOperatorRuntimeEnvironment(final String processingNodeId, final String pipelineId, final DelayedResponseOperator delayedResponseOperator, final DelayedResponseOperatorWaitStrategy responseWaitStrategy,
			final StreamingMessageQueueConsumer queueConsumer, final StreamingMessageQueueProducer queueProducer, 
			final ExecutorService executorService) throws RequiredInputMissingException {
		
		/////////////////////////////////////////////////////////////
		// input validation
		if(StringUtils.isBlank(processingNodeId))
			throw new RequiredInputMissingException("Missing required processing node identifier");
		if(StringUtils.isBlank(pipelineId))
			throw new RequiredInputMissingException("Missing required pipeline identifier");
		if(delayedResponseOperator == null)
			throw new RequiredInputMissingException("Missing required direct delayed operator");
		if(responseWaitStrategy == null)
			throw new RequiredInputMissingException("Missing required response wait strategy");
		if(queueConsumer == null)
			throw new RequiredInputMissingException("Missing required queue consumer");
		if(queueProducer == null)
			throw new RequiredInputMissingException("Missing required queue producer");
		//
		/////////////////////////////////////////////////////////////

		this.processingNodeId = StringUtils.lowerCase(StringUtils.trim(processingNodeId));
		this.pipelineId = StringUtils.lowerCase(StringUtils.trim(pipelineId));
		this.operatorId = StringUtils.lowerCase(StringUtils.trim(delayedResponseOperator.getId()));

		this.delayedResponseOperator = delayedResponseOperator;
		this.responseWaitStrategy = responseWaitStrategy;
		this.responseWaitStrategy.setDelayedResponseCollector(this);
		this.delayedResponseOperator.setWaitStrategy(this.responseWaitStrategy);
		this.queueConsumer = queueConsumer;
		this.queueProducer = queueProducer;
		this.executorService = executorService;		
		this.executorService.submit(this.responseWaitStrategy);
		this.running = true;
		this.consumerQueueWaitStrategy = queueConsumer.getWaitStrategy();
		this.destinationQueueWaitStrategy = queueProducer.getWaitStrategy();

		if(logger.isDebugEnabled())
			logger.debug("delayed response operator init [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]");
	}
	
	/**
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
		while(running) {

			try {
				StreamingDataMessage message = this.consumerQueueWaitStrategy.waitFor(this.queueConsumer); // this.queueConsumer.next();
				if(message != null && message.getBody() != null) {
					// forward retrieved message to operator for further processing
					this.delayedResponseOperator.onMessage(message);
					// notify response wait strategy on retrieved message
					this.responseWaitStrategy.onMessage(message);
					
					if(this.messageCounter != null)
						this.messageCounter.inc();
				}
		} catch(InterruptedException e) {
				// do nothing - waiting was interrupted				
			} catch(Exception e) {
				logger.error("processing error [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]: " + e.getMessage(), e);
				// TODO add handler for responding to errors
			}
		}
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseCollector#retrieveMessages()
	 */
	public void retrieveMessages() {		
		try {		
			// try to fetch messages from underlying operator
			StreamingDataMessage[] retrievedMessages = this.delayedResponseOperator.getResult();

			// forward messages to assigned queue if any messages are available 
			if(retrievedMessages != null) {
				for(StreamingDataMessage rm : retrievedMessages)
					this.queueProducer.insert(rm);
				this.destinationQueueWaitStrategy.forceLockRelease();
			}
		} catch(Exception e) {
			logger.error("message retrieval error [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]: " + e.getMessage(), e);
			// TODO add handler for responding to errors 
		}
	}

	/**
	 * Shuts down the runtime environment as well as the attached {@link Operator}
	 */
	public void shutdown() {
		this.running = false;
		try {
			this.delayedResponseOperator.shutdown();
		} catch(Exception e) {
			logger.error("operator shutdown error [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]: " + e.getMessage(), e);
		}
		try {
			this.responseWaitStrategy.shutdown();
		} catch(Exception e) {
			logger.error("strategy shutdown error [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]: " + e.getMessage(), e);
		}
		if(this.localExecutorService) {
			try {
				this.executorService.shutdownNow();
			} catch(Exception e) {
				logger.error("exec service shutdown error [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]: " + e.getMessage(), e);
			}
		}


		if(logger.isDebugEnabled()) {
			logger.debug("shutdown success [node="+this.processingNodeId+", pipeline="+this.pipelineId+", operator="+this.operatorId+"]");
		}
	}

	/**
	 * @return the running
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * @param messageCounter the messageCounter to set
	 */
	public void setMessageCounter(Counter messageCounter) {
		this.messageCounter = messageCounter;
	}
}
