package com.ottogroup.bi.spqr.operator.json.aggregator;

import com.fasterxml.jackson.databind.JsonNode;
import com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator;

public class SuperclassExtracted {

	public SuperclassExtracted() {
		super();
	}

	/**
	 * Walks along the path provided and reads out the leaf value which is returned as string 
	 * @param jsonNode
	 * @param fieldPath
	 * @return
	 */
	public String getTextFieldValue(final JsonNode jsonNode, final String[] fieldPath) {
	
		int fieldAccessStep = 0;
		JsonNode contentNode = jsonNode;
		while(fieldAccessStep < fieldPath.length) {
			contentNode = contentNode.get(fieldPath[fieldAccessStep]);
			fieldAccessStep++;
		}	
	
		return contentNode.textValue();
	}

}