package eu.dnetlib.iis.workflows.importer.content;

import java.io.IOException;

/**
 * Content provider interface.
 * @author mhorst
 *
 */
public interface ContentProviderService {

	/**
	 * Retrieves content for given id.
	 * @param id
	 * @param repositoryUrls repository identifier with url locations
	 * @return byte[]
	 * @throws IOException 
	 */
	byte[] getContent(String id, 
			RepositoryUrls[] repositoryUrls) throws IOException;
	
}
