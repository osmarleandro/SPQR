/**
 * Copyright 2014 Otto (GmbH & Co KG)
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

import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;


/**
 * Receives an input {@link StreamingDataMessage message} but defers its results to be fetched later, eg. aggregate or group operator. The
 * configuration must reference a {@link DelayedResponseOperatorWaitStrategy} to be applied when executing this operator. The strategy must
 * be referenced inside the operator settings using the key named by {@link StreamingDataMessage#CFG_WAIT_STRATEGY_NAME}. Additional
 * settings required for strategy initialization must be provided using the content of {@link DelayedResponseOperator#CFG_WAIT_STRATEGY_SETTINGS_PREFIX}
 * as prefix followed by the options name and its value, eg. waitStrategy.cfg.testKey=testValue. 
 * @author mnxfst
 * @since Dec 14, 2014
 * TODO exporting delayed result may be triggered by internal timer/counter which is provided from the outside?!
 */
public interface DelayedResponseOperator extends Operator {

	public static final String CFG_WAIT_STRATEGY_SETTINGS_PREFIX = "waitStrategy.cfg.";
	
	/**
	 * Provides a new message to the operator
	 * @param message
	 */
	public void onMessage(final StreamingDataMessage message);
	
	/**
	 * Accesses the result generated by the operator
	 * @return
	 */
	public StreamingDataMessage[] getResult();
	
	/**
	 * Returns the number of messages processed since the last call
	 * of {@link DelayedResponseOperator#getResult()}
	 * @return
	 */
	public long getNumberOfMessagesSinceLastResult();
	
	/**
	 * Assigns the {@link DelayedResponseOperatorWaitStrategy} used for triggering result retrieval 
	 * @param waitStrategy
	 */
	public void setWaitStrategy(final DelayedResponseOperatorWaitStrategy waitStrategy);
}
