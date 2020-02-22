package com.ottogroup.bi.spqr.operator.json.aggregator;

import com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;

public class SuperclassExtracted {

	public SuperclassExtracted() {
		super();
	}

	/**
	 * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#onMessage(com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage)
	 * @deprecated Use {@link com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage#onMessage(com.ottogroup.bi.spqr.operator.json.aggregator.JsonContentAggregator)} instead
	 */
	public void onMessage(StreamingDataMessage message) {
		message.onMessage(this);
	}

}