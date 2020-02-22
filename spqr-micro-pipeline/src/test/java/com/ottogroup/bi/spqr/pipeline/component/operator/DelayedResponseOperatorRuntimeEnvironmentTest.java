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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueConsumer;
import com.ottogroup.bi.spqr.pipeline.queue.StreamingMessageQueueProducer;
import com.ottogroup.bi.spqr.pipeline.queue.strategy.StreamingMessageQueueWaitStrategy;

/**
 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment}
 * @author mnxfst
 * @since Mar 11, 2015
 */
public class DelayedResponseOperatorRuntimeEnvironmentTest {

	private final static ExecutorService executorService = Executors.newCachedThreadPool();
	
	@AfterClass
	public static void shutdown() {
		if(executorService != null)
			executorService.shutdownNow();
	}
	
	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to processing node id
	 */
	@Test
	public void testConstructor_withNullProcessingNodeId() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment(null, "pipe-1", Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}
	
	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to pipeline id
	 */
	@Test
	public void testConstructor_withNullPipelineId() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", null, Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}
	
	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to operator parameter which must lead to a {@link RequiredInputMissingException}
	 */
	@Test
	public void testConstructor_withNullOperatorInput() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", null, Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}
	
	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to wait strategy parameter which must lead to a {@link RequiredInputMissingException}
	 */
	@Test
	public void testConstructor_withNullWaitStrategyInput() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", Mockito.mock(DelayedResponseOperator.class), null, Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}

	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to queue consumer parameter which must lead to a {@link RequiredInputMissingException}
	 */
	@Test
	public void testConstructor_withNullConsumerInput() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), null, Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}

	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to queue producer parameter which must lead to a {@link RequiredInputMissingException}
	 */
	@Test
	public void testConstructor_withNullProducerInput() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), null, Mockito.mock(StreamingMessageQueueProducer.class), 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}

	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided null as input to stats queue producer parameter which must lead to a {@link RequiredInputMissingException}
	 */
	@Test
	public void testConstructor_withNullStatsQueueProducerInput() {
		try {
			new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), null, 100);
			Assert.fail("Missing required input");
		} catch(RequiredInputMissingException e) {
			// expected
		}
	}
	
	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment#DelayedResponseOperatorRuntimeEnvironment(DelayedResponseOperator, DelayedResponseOperatorWaitStrategy, StreamingMessageQueueConsumer, StreamingMessageQueueProducer)}
	 * being provided valid input. The {@link DelayedResponseOperatorRuntimeEnvironment#isRunning()} method must show true now
	 */
	@Test
	public void testConstructor_withValidInput() throws RequiredInputMissingException  {
		DelayedResponseOperatorRuntimeEnvironment env = new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", Mockito.mock(DelayedResponseOperator.class), Mockito.mock(DelayedResponseOperatorWaitStrategy.class), Mockito.mock(StreamingMessageQueueConsumer.class), Mockito.mock(StreamingMessageQueueProducer.class), Mockito.mock(StreamingMessageQueueProducer.class), 100);
		Assert.assertTrue("The environment must be running", env.isRunning());
	}

	/**
	 * Test case for {@link DelayedResponseOperatorRuntimeEnvironment} being provided valid input and some messages
	 * which must be processed without errors
	 */
	@Test
	public void test_withValidInputAndMessages() throws RequiredInputMissingException, InterruptedException {
		DelayedResponseOperator delayedResponseOperator = Mockito.mock(DelayedResponseOperator.class);		
		DelayedResponseOperatorWaitStrategy responseWaitStrategy = Mockito.mock(DelayedResponseOperatorWaitStrategy.class);
		StreamingMessageQueueConsumer queueConsumer = Mockito.mock(StreamingMessageQueueConsumer.class);
		StreamingMessageQueueProducer queueProducer = Mockito.mock(StreamingMessageQueueProducer.class);
		StreamingMessageQueueProducer statsQueueProducer = Mockito.mock(StreamingMessageQueueProducer.class);
		StreamingMessageQueueWaitStrategy queueConsumerWaitStrategy = Mockito.mock(StreamingMessageQueueWaitStrategy.class);
		StreamingMessageQueueWaitStrategy queueProducerWaitStrategy = Mockito.mock(StreamingMessageQueueWaitStrategy.class);
		
		StreamingDataMessage message = new StreamingDataMessage("test-message".getBytes(), System.currentTimeMillis());		
		StreamingDataMessage response = new StreamingDataMessage("response-test-message".getBytes(), System.currentTimeMillis());
		Mockito.when(queueConsumer.getWaitStrategy()).thenReturn(queueConsumerWaitStrategy);
		Mockito.when(queueConsumerWaitStrategy.waitFor(queueConsumer)).thenReturn(message);
		Mockito.when(delayedResponseOperator.getResultRenamed()).thenReturn(new StreamingDataMessage[]{response});
		Mockito.when(delayedResponseOperator.getId()).thenReturn("test-id");
		Mockito.when(queueProducer.getWaitStrategy()).thenReturn(queueProducerWaitStrategy);
		
		DelayedResponseOperatorRuntimeEnvironment env = new DelayedResponseOperatorRuntimeEnvironment("proc-1", "pipe-1", delayedResponseOperator, responseWaitStrategy, queueConsumer, queueProducer, statsQueueProducer, 100);
		executorService.submit(env);

		env.retrieveMessages();

		Mockito.verify(queueConsumer).getWaitStrategy();

		Mockito.verify(queueConsumerWaitStrategy, Mockito.timeout(500).atLeastOnce()).waitFor(queueConsumer);
		Mockito.verify(delayedResponseOperator, Mockito.timeout(500).atLeast(1)).onMessage(message);
		Mockito.verify(responseWaitStrategy, Mockito.timeout(500).atLeast(1)).onMessage(message);
		Mockito.verify(delayedResponseOperator, Mockito.timeout(500)).getResultRenamed();
		Mockito.verify(queueProducerWaitStrategy, Mockito.timeout(500)).forceLockRelease();
		Mockito.verify(queueProducer, Mockito.timeout(500)).insert(response);
		Mockito.verify(statsQueueProducer, Mockito.timeout(500).atLeastOnce()).insert(Mockito.any(StreamingDataMessage.class));
		
		Assert.assertTrue("The environment must be running", env.isRunning());
		env.shutdown();
	}
}
