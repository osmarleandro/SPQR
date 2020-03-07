package com.ottogroup.bi.spqr.pipeline;

public interface ExtractedInterface {

	/**
	 * Validates the contents of a provided {@link MicroPipelineConfiguration} for being compliant with a required format
	 * and errors that may be inferred from provided contents
	 * @param configuration
	 * @return
	 */
	MicroPipelineValidationResult validate(MicroPipelineConfiguration configuration);

}