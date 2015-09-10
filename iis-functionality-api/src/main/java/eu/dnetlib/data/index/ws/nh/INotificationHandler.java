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
package eu.dnetlib.data.index.ws.nh;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebResult;

/**
 * Notification consumer methods.
 * @author Marek Horst
 * @version 0.0.1
 *
 */
@Deprecated
public interface INotificationHandler {

	/**
	 * Notification consumer method.
	 * @param subscrId
	 * @param topic
	 * @param isId
	 * @param message
	 * @return Return
	 */
	@WebMethod(operationName="notify", action="notify")
	@WebResult(name = "Return")
	public boolean notify(
			@WebParam(name="subscrId") String subscrId, 
			@WebParam(name="topic") String topic, 
			@WebParam(name="is_id") String isId,
			@WebParam(name="message") String message);
}
