package eu.dnetlib.iis.importer.content;

import java.io.IOException;



/**
 * Simple content provider implementation returning 
 * static predefined content set to null by default.
 * @author mhorst
 *
 */
public class StaticContentProviderService implements ContentProviderService {

	private byte[] predefinedContent = null;
	
	@Override
	public byte[] getContent(String id,
			RepositoryUrls[] repositoryUrls) throws IOException {
		return predefinedContent;
	}

	/**
	 * Sets predefined content.
	 * @param predefinedContent
	 */
	public void setPredefinedContent(byte[] predefinedContent) {
		this.predefinedContent = predefinedContent;
	}

}
