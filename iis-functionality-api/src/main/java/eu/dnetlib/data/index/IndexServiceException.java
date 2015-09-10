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

/**
 * General index service exception.
 * @author Marek Horst
 * @version 0.0.1
 *
 */
@Deprecated
public class IndexServiceException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6772977735282310658L;

	public IndexServiceException(java.lang.String arg0, java.lang.Throwable arg1) {
		super(arg0, arg1);
	}

	public IndexServiceException(java.lang.String arg0) {
		super(arg0);
	}

	public IndexServiceException(java.lang.Throwable arg0) {
		super(arg0);
	}

	public IndexServiceException() {
		super();
	}
	
}
