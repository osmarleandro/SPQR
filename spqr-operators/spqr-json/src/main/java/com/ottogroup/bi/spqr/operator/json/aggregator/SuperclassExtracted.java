package com.ottogroup.bi.spqr.operator.json.aggregator;

import com.fasterxml.jackson.databind.JsonNode;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.operator.json.JsonContentType;
import com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SuperclassExtracted {
    public String ATTRIBUTE_TO_PUSH_DOWN = "ATTRIBUTE_TO_PUSH_DOWN";

    /** component identifier assigned by caller */
    protected String id = null;

    /**
     * Walks along the path provided and reads out the leaf value which is returned as string
     * @param jsonNode
     * @param fieldPath
     * @return
     */
    protected String getTextFieldValue(final JsonNode jsonNode, final String[] fieldPath) {

        int fieldAccessStep = 0;
        JsonNode contentNode = jsonNode;
        while(fieldAccessStep < fieldPath.length) {
            contentNode = contentNode.get(fieldPath[fieldAccessStep]);
            fieldAccessStep++;
        }

        return contentNode.textValue();
    }
}
