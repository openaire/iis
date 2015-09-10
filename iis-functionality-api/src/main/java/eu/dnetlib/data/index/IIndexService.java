/**
 * Copyright 2008-2009 DRIVER PROJECT (ICM UW)
 * Original author: Marek Horst
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.dnetlib.data.index;


import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.xml.ws.wsaddressing.W3CEndpointReference;

import eu.dnetlib.common.ws.dataprov.IDataProviderExt;
import eu.dnetlib.data.index.ws.nh.INotificationHandler;

/**
 * Main Driver Index Service interface.
 * @author Marek Horst
 * @version 0.0.1
 *
 */
@WebService(targetNamespace = "http://services.dnetlib.eu/")
@SOAPBinding(style = SOAPBinding.Style.DOCUMENT, use = SOAPBinding.Use.LITERAL, 
		parameterStyle=SOAPBinding.ParameterStyle.WRAPPED)
@Deprecated
public interface IIndexService extends INotificationHandler, IDataProviderExt {

	/**
	 * Identifies service and version.
	 * @return service version identifier
	 */
	@WebMethod(operationName="identify", action="identify")
	public String identify();
	
	/**
	 * Returns ResultSet EPR for the index lookup result.
	 * TODO make sure that WebParam names are correct, according to wsdl which is apparently not specified yet
	 * @param ixId
	 * @param query
	 * @param mdFormatId
	 * @param layoutId
	 * @throws IndexServiceException
	 * @return ResultSet EPR for the index lookup result
	 */
	@WebMethod(operationName="indexLookup", action="indexLookup")
	public W3CEndpointReference indexLookup(
			@WebParam(name="id") String ixId, 
			@WebParam(name="query") String query, 
			@WebParam(name="mdformat") String mdFormatId,
			@WebParam(name="layout") String layoutId)
		throws IndexServiceException;
	
	/**
	 * Returns index statistics.
	 * @param ixId
	 * @throws IndexServiceException
	 * @return index statistics
	 */
	@WebMethod(operationName="getIndexStatistics", action="getIndexStatistics")
	public String getIndexStatistics(
			@WebParam(name="index") String ixId)
		throws IndexServiceException;
	
	/**
	 * Returns browsing statistics.
	 * @param query
	 * @param ixId
	 * @throws IndexServiceException
	 * @return browsing statistics
	 */
	@WebMethod(operationName="getBrowsingStatistics", action="getBrowsingStatistics")
	public W3CEndpointReference getBrowsingStatistics(
			@WebParam(name="query") String query, 
			@WebParam(name="index") String ixId,
			@WebParam(name="mdformat") String mdFormatId,
			@WebParam(name="layout") String layoutId)
		throws IndexServiceException;
	
	/**
	 * Returns list of all stored indices.
	 * @return list of all stored indices
	 */
	@WebMethod(operationName="getListOfIndices", action="getListOfIndices")
	public String[] getListOfIndices();
	
	/**
	 * Returns list of all stored indices in CSV format.
	 * @return list of all stored indices in CSV format
	 */
	@WebMethod(operationName="getListOfIndicesCSV", action="getListOfIndicesCSV")
	public String getListOfIndicesCSV();
	
}
