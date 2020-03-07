package com.ottogroup.bi.spqr.pipeline;

import com.ottogroup.bi.spqr.pipeline.component.MicroPipelineConfigurationRenamed;

public interface ExtractedInterface {

	/**
	 * Validates the contents of a provided {@link MicroPipelineConfigurationRenamed} for being compliant with a required format
	 * and errors that may be inferred from provided contents
	 * @param configuration
	 * @return
	 */
	MicroPipelineValidationResult validate(MicroPipelineConfigurationRenamed configuration);

}