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
package com.ottogroup.bi.spqr.pipeline.queue.chronicle;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.codahale.metrics.Counter;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueConsumer;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueProducer;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueBlockingWaitStrategy;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueDirectPassStrategy;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueSleepingWaitStrategy;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueWaitStrategy;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

/**
 * Implements a {@link StreamingMessageQueue} based on {@link Chronicle}
 * @author mnxfst
 * @since Mar 5, 2015
 */
public class DefaultStreamingMessageQueue implements StreamingMessageQueue {


	/** our faithful logging facility ... ;-) */
	private static final Logger logger = Logger.getLogger(DefaultStreamingMessageQueue.class);
	
	public static final String CFG_CHRONICLE_QUEUE_PATH = "queue.chronicle.path";
	public static final String CFG_CHRONICLE_QUEUE_DELETE_ON_EXIT = "queue.chronicle.deleteOnExist";
	public static final String CFG_CHRONICLE_QUEUE_ROLLING_INTERVAL = "queue.chronicle.rollingInterval";
	public static final String CFG_CHRONICLE_QUEUE_CYCLE_FORMAT = "queue.chronicle.cycleFormat";
	public static final String CFG_QUEUE_MESSAGE_WAIT_STRATEGY = "queue.message.waitStrategy";

	/** unique queue identifier */
	private String id = null;
	/** base path to use when creating/accessing the referenced chronicle files */
	private String basePath = null;
	/** delete the chronicle files on start and shutdown */
	private boolean deleteOnExit = true;
	/** chronicle file rolling interval - provided in minutes */
	private long queueRollingInterval = TimeUnit.MINUTES.toMillis(60);
	/** cycle format - default: yyyy-MM-dd/HH-mm */
	private String cycleFormat = "yyyy-MM-dd-HH-mm";
	/** chronicle instance - required for creating and accessing appender & tailer */
	private Chronicle chronicle = null;	
	/** provides read access to chronicle */
	private DefaultStreamingMessageQueueConsumer queueConsumer = null;
	/** provides write access to chronicle */
	private DefaultStreamingMessageQueueProducer queueProducer = null;
	/** wait strategy applied on this queue */
	private StreamingMessageQueueWaitStrategy queueWaitStrategy = null;

	public long getSize() {
		return chronicle.size();
	}
	
	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#initialize(java.util.Properties)
	 */
	public void initialize(Properties properties) throws RequiredInputMissingException {

		////////////////////////////////////////////////////////////////////////////////
		// extract and validate input
		if(properties == null)
			throw new RequiredInputMissingException("Missing required properties");		
		
		if(StringUtils.isBlank(this.id))
			throw new RequiredInputMissingException("Missing required queue identifier");

		if(StringUtils.equalsIgnoreCase(StringUtils.trim(properties.getProperty(CFG_CHRONICLE_QUEUE_DELETE_ON_EXIT)), "false"))
			this.deleteOnExit = false;

		this.basePath = StringUtils.lowerCase(StringUtils.trim(properties.getProperty(CFG_CHRONICLE_QUEUE_PATH)));
		if(StringUtils.isBlank(this.basePath))
			this.basePath = System.getProperty("java.io.tmpdir");
		
		String tmpCycleFormat = StringUtils.trim(properties.getProperty(CFG_CHRONICLE_QUEUE_CYCLE_FORMAT, "yyyy-MM-dd-HH-mm"));
		if(StringUtils.isNotBlank(tmpCycleFormat))
			this.cycleFormat = tmpCycleFormat;

		String pathToChronicle = this.basePath;
		if(!StringUtils.endsWith(pathToChronicle, File.separator))
			pathToChronicle = pathToChronicle + File.separator;
		pathToChronicle = pathToChronicle + id;
		
		try {
			this.queueRollingInterval = TimeUnit.MINUTES.toMillis(Long.parseLong(StringUtils.trim(properties.getProperty(CFG_CHRONICLE_QUEUE_ROLLING_INTERVAL))));
			
			if(this.queueRollingInterval < VanillaChronicle.MIN_CYCLE_LENGTH) {
				this.queueRollingInterval = VanillaChronicle.MIN_CYCLE_LENGTH;
			}
		} catch(Exception e) {
			logger.info("Invalid queue rolling interval found: " + e.getMessage() + ". Using default: " + TimeUnit.MINUTES.toMillis(60));
		}
		
		this.queueWaitStrategy = getWaitStrategy(StringUtils.trim(properties.getProperty(CFG_QUEUE_MESSAGE_WAIT_STRATEGY)));
		
		//
		////////////////////////////////////////////////////////////////////////////////
		
		// clears the queue if requested 
		if(this.deleteOnExit) { 
			ChronicleTools.deleteDirOnExit(pathToChronicle);
			ChronicleTools.deleteOnExit(pathToChronicle);
		}
		
        try {
        	this.chronicle = ChronicleQueueBuilder.vanilla(pathToChronicle).cycleLength((int)this.queueRollingInterval).cycleFormat(this.cycleFormat).build();
        	this.queueConsumer = new DefaultStreamingMessageQueueConsumer(this.getId(), this.chronicle.createTailer(), this.queueWaitStrategy);
			this.queueProducer = new DefaultStreamingMessageQueueProducer(this.getId(), this.chronicle.createAppender(), this.queueWaitStrategy);
		} catch (IOException e) {
			throw new RuntimeException("Failed to initialize chronicle at '"+pathToChronicle+"'. Error: " + e.getMessage());
		}
        
        logger.info("queue[type=chronicle, id="+this.id+", deleteOnExist="+this.deleteOnExit+", path="+pathToChronicle+"']");       		
	}

	/**
	 * Return an instance of the referenced {@link StreamingMessageQueueWaitStrategy}
	 * @param waitStrategyName name of strategy to instantiate (eg. {@link StreamingMessageQueueBlockingWaitStrategy#STRATEGY_NAME} (default))
	 * @return
	 */
	protected StreamingMessageQueueWaitStrategy getWaitStrategy(final String waitStrategyName) {			
		if(StringUtils.equalsIgnoreCase(waitStrategyName, StreamingMessageQueueDirectPassStrategy.STRATEGY_NAME))
			return new StreamingMessageQueueDirectPassStrategy();
		else if(StringUtils.equalsIgnoreCase(waitStrategyName, StreamingMessageQueueSleepingWaitStrategy.STRATEGY_NAME))
			return new StreamingMessageQueueSleepingWaitStrategy();
		return new StreamingMessageQueueBlockingWaitStrategy();
	}
	
	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#shutdown()
	 */
	public boolean shutdown() {
		try {
			this.chronicle.close();
			return true;
		} catch (IOException e) {
			logger.error("Failed to close underlying chronicle at " + basePath + "/"+id+". Error: " + e.getMessage());;
		}
		return false;
	}
	
	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#insert(com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage)
	 */
	public boolean insert(StreamingDataMessage message) {
		return queueProducer.insert(message); // TODO access appender directly
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#next()
	 */
	public StreamingDataMessage next() {
		return queueConsumer.next(); // TODO access tailer directly
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#getProducer()
	 */
	public StreamingMessageQueueProducer getProducer() {		
		return this.queueProducer;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#getConsumer()
	 */
	public StreamingMessageQueueConsumer getConsumer() {
		return this.queueConsumer;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#setId(java.lang.String)
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#getId()
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#setMessageInsertionCounter(com.codahale.metrics.Counter)
	 */
	public void setMessageInsertionCounter(Counter counter) {
		this.queueProducer.setMessageInsertionCounter(counter);
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueue#setMessageRetrievalCounter(com.codahale.metrics.Counter)
	 */
	public void setMessageRetrievalCounter(Counter counter) {
		this.queueConsumer.setMessageRetrievalCounter(counter);
	}

	

}
