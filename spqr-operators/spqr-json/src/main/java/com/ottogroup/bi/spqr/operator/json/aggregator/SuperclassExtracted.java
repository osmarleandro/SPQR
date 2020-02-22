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
     * @see com.ottogroup.bi.spqr.pipeline.component.operator.DelayedResponseOperator#onMessage(com.ottogroup.bi.spqr.pipeline.message.StreamingDataMessage)
     */
    public void onMessage(StreamingDataMessage message) {
        this.messageCount++;
        this.messagesSinceLastResult++;


        // do nothing if either the event or the body is empty
        if(message == null || message.getBody() == null || message.getBody().length < 1)
            return;

        JsonNode jsonNode = null;
        try {
            jsonNode = jsonMapper.readTree(message.getBody());
        } catch(IOException e) {
            JsonContentAggregator.logger.error("Failed to read message body to json node. Ignoring message. Error: " + e.getMessage());
        }

        // return null in case the message could not be parsed into
        // an object representation - the underlying processor does
        // not forward any NULL messages
        if(jsonNode == null)
            return;

        // initialize the result document if not already done
        if(this.resultDocument == null)
            this.resultDocument = new JsonContentAggregatorResult(this.pipelineId, this.documentType);

        Map<String, Object> rawData = new HashMap<>();
        // step through fields considered to be relevant, extract values and apply aggregation function
        for(final JsonContentAggregatorFieldSetting fieldSettings : fields) {

            // switch between string and numerical field values
            // string values may be counted only
            // numerical field values must be summed, min and max computed AND counted

            // string values may be counted only
            if(fieldSettings.getValueType() == JsonContentType.STRING) {

                try {
                    // read value into string representation and add it to raw data dump
                    String value = getTextFieldValue(jsonNode, fieldSettings.getPath());
                    if(storeForwardRawData)
                        rawData.put(fieldSettings.getField(), value);

                    // count occurrences of value
                    try {
                        this.resultDocument.incAggregatedValue(fieldSettings.getField(), value, 1);
                    } catch (RequiredInputMissingException e) {
                        JsonContentAggregator.logger.error("Field '"+fieldSettings.getField()+"' not found in event. Ignoring value. Error: " +e.getMessage());
                    }
                } catch(Exception e) {
                }
            } else if(fieldSettings.getValueType() == JsonContentType.NUMERICAL) {

                try {
                    // read value into numerical representation and add it to raw data map
                    long value = getNumericalFieldValue(jsonNode, fieldSettings.getPath());
                    if(storeForwardRawData)
                        rawData.put(fieldSettings.getField(), value);

                    // compute min, max and sum and add these values to result document
                    try {
                        this.resultDocument.evalMinAggregatedValue(fieldSettings.getField(), "min", value);
                        this.resultDocument.evalMaxAggregatedValue(fieldSettings.getField(), "max", value);
                        this.resultDocument.incAggregatedValue(fieldSettings.getField(), "sum", value);
                    } catch(RequiredInputMissingException e) {
                        JsonContentAggregator.logger.error("Field '"+fieldSettings.getField()+"' not found in event. Ignoring value. Error: " +e.getMessage());
                    }

                } catch(Exception e) {
                }
            }
        }

        // add raw data to document
        if(storeForwardRawData)
            this.resultDocument.addRawData(rawData);
    }
}
