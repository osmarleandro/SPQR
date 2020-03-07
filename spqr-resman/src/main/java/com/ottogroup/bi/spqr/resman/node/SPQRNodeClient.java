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
package com.ottogroup.bi.spqr.resman.node;

import java.io.IOException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.ottogroup.bi.spqr.exception.RemoteClientConnectionFailedException;
import com.ottogroup.bi.spqr.exception.RequiredInputMissingException;
import com.ottogroup.bi.spqr.node.resource.MicroPipelineInstantiationResponse;
import com.ottogroup.bi.spqr.node.resource.pipeline.MicroPipelineShutdownResponse;
import com.ottogroup.bi.spqr.node.resource.pipeline.MicroPipelineShutdownResponse.MicroPipelineShutdownState;
import com.ottogroup.bi.spqr.pipeline.ExtractedInterface;
import com.ottogroup.bi.spqr.pipeline.MicroPipelineConfiguration;
import com.ottogroup.bi.spqr.pipeline.MicroPipelineValidationResult;
import com.ottogroup.bi.spqr.pipeline.MicroPipelineValidator;

/**
 * Communication client used for accessing remote processing nodes for issuing pipeline instantiations
 * or requesting statistics  
 * @author mnxfst
 * @since Apr 13, 2015
 * TODO implement method for retrieving "alive" state from node
 * TODO implement method for retrieving statistics from node
 */
public class SPQRNodeClient {
	
	/** our faithful logging service .... ;-) */
	private static final Logger logger = Logger.getLogger(SPQRNodeClient.class);
	
	/** client to be used for accessing remote processing node */
	private final Client restClient;
	/** base url for accessing service api */
	private final String processingNodeServiceBaseUrl;
	/** base url for accessing admin api */
	private final String processingNodeAdminBaseUrl;
	/** pipeline configuration validator */
	private final ExtractedInterface pipelineConfigurationValidator = new MicroPipelineValidator();

	/**
	 * Initializes the node client using the provided input
	 * @param protocol
	 * @param remoteHost
	 * @param servicePort
	 * @param adminPort
	 * @param client
	 * @throws RequiredInputMissingException
	 */
	public SPQRNodeClient(final String protocol, final String remoteHost, final int servicePort, final int adminPort, final Client client) throws RequiredInputMissingException {
		
		///////////////////////////////////////////////////////////////
		// validate input
		if(StringUtils.isBlank(protocol))
			throw new RequiredInputMissingException("Missing required protocol");
		if(StringUtils.isBlank(remoteHost))
			throw new RequiredInputMissingException("Missing required remote host");
		if(servicePort < 1)
			throw new RequiredInputMissingException("Missing required service port");
		if(adminPort < 1)
			throw new RequiredInputMissingException("Missing required admin port");
		if(client == null)
			throw new RequiredInputMissingException("Missing required client");
		//
		///////////////////////////////////////////////////////////////
		
		this.processingNodeServiceBaseUrl = new StringBuffer(protocol).append("://").append(remoteHost).append(":").append(servicePort).toString();
		this.processingNodeAdminBaseUrl = new StringBuffer(protocol).append("://").append(remoteHost).append(":").append(adminPort).toString();
		this.restClient = client;
		
		if(logger.isDebugEnabled())
			logger.debug("rest client[protocol="+protocol+", host="+remoteHost+", servicePort="+servicePort+", adminPort="+adminPort+"]");
	}
	
	/**
	 * Instantiates the referenced pipeline on the remote processing node
	 * @param pipelineConfiguration
	 * @return
	 * @throws RequiredInputMissingException
	 * @throws IOException
	 * @throws RemoteClientConnectionFailedException
	 */
	public MicroPipelineInstantiationResponse instantiatePipeline(final MicroPipelineConfiguration pipelineConfiguration) 
			throws IOException, RemoteClientConnectionFailedException {
				
		///////////////////////////////////////////////////////
		// validate input for not being null as the content
		MicroPipelineValidationResult cfgValidationResult = this.pipelineConfigurationValidator.validate(pipelineConfiguration);
		if(cfgValidationResult != MicroPipelineValidationResult.OK)
			return new MicroPipelineInstantiationResponse("", cfgValidationResult, "Failed to validate pipeline configuration");
		if(StringUtils.isBlank(pipelineConfiguration.getId()))
			return new MicroPipelineInstantiationResponse("", MicroPipelineValidationResult.MISSING_PIPELINE_ID, "Failed to generated unique pipeline identifier");
		//
		///////////////////////////////////////////////////////		
		
		StringBuffer url = new StringBuffer(this.processingNodeServiceBaseUrl).append("/pipelines");
		
		if(logger.isDebugEnabled()) 
			logger.debug("Instantiating pipeline [id="+pipelineConfiguration.getId()+"] on processing node " + url.toString());
		
		try {
			
			final WebTarget webTarget = this.restClient.target(url.toString());			
			return webTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).post(Entity.entity(pipelineConfiguration, MediaType.APPLICATION_JSON), MicroPipelineInstantiationResponse.class);
		} catch(Exception e) {
			throw new RemoteClientConnectionFailedException("Failed to establish a connection with the remote resource manager [url="+url.toString()+"]. Error: " + e.getMessage());
		}
	}	
	

	/**
	 * Instantiates or updates the referenced pipeline on the remote processing node
	 * @param pipelineConfiguration
	 * @return
	 * @throws RequiredInputMissingException
	 * @throws IOException
	 * @throws RemoteClientConnectionFailedException
	 */
	public MicroPipelineInstantiationResponse updatePipeline(final MicroPipelineConfiguration pipelineConfiguration) 
			throws IOException, RemoteClientConnectionFailedException {
		
		///////////////////////////////////////////////////////
		// validate input
		MicroPipelineValidationResult cfgValidationResult = this.pipelineConfigurationValidator.validate(pipelineConfiguration);
		if(cfgValidationResult != MicroPipelineValidationResult.OK)
			return new MicroPipelineInstantiationResponse("", cfgValidationResult, "Failed to validate pipeline configuration");
		if(StringUtils.isBlank(pipelineConfiguration.getId()))
			return new MicroPipelineInstantiationResponse("", MicroPipelineValidationResult.MISSING_PIPELINE_ID, "Failed to generated unique pipeline identifier");
		//
		///////////////////////////////////////////////////////

		StringBuffer url = new StringBuffer(this.processingNodeServiceBaseUrl).append("/pipelines/").append(pipelineConfiguration.getId());
		
		if(logger.isDebugEnabled()) 
			logger.debug("Updating or instantiating pipeline [id="+pipelineConfiguration.getId()+"] on processing node " + url.toString());
		
		try {
			final WebTarget webTarget = this.restClient.target(url.toString());			
			return webTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).put(Entity.entity(pipelineConfiguration, MediaType.APPLICATION_JSON), MicroPipelineInstantiationResponse.class);
		} catch(Exception e) {
			throw new RemoteClientConnectionFailedException("Failed to establish a connection with the remote resource manager [url="+url.toString()+"]. Error: " + e.getMessage());
		}		
	}
	
	/**
	 * Shuts down the referenced pipeline on the remote processing node
	 * @param pipelineId
	 * @return
	 * @throws RequiredInputMissingException
	 * @throws RemoteClientConnectionFailedException
	 */
	public MicroPipelineShutdownResponse shutdown(final String pipelineId) throws RemoteClientConnectionFailedException {
		
		///////////////////////////////////////////////////////
		// validate input
		if(StringUtils.isBlank(pipelineId))
			return new MicroPipelineShutdownResponse(pipelineId, MicroPipelineShutdownState.PIPELINE_ID_MISSING, "Missing required pipeline identifier");
		//
		///////////////////////////////////////////////////////

		StringBuffer url = new StringBuffer(this.processingNodeServiceBaseUrl).append("/pipelines/").append(pipelineId);

		if(logger.isDebugEnabled()) 
			logger.debug("Deleting pipeline [id="+pipelineId+"] on processing node " + url.toString());
		
		try {
			final WebTarget webTarget = this.restClient.target(url.toString());			
			return webTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).delete(MicroPipelineShutdownResponse.class);
		} catch(Exception e) {
			throw new RemoteClientConnectionFailedException("Failed to establish a connection with the remote resource manager [url="+url.toString()+"]. Error: " + e.getMessage());
		}		

	}

	/**
	 * Called by {@link SPQRNodeManager} on client de-registration 
	 */
	public void shutdown() {
		
	}
	
	protected String getProcessingNodeServiceBaseUrl() {
		return processingNodeServiceBaseUrl;
	}

	protected String getProcessingNodeAdminBaseUrl() {
		return processingNodeAdminBaseUrl;
	}
	
	
}
