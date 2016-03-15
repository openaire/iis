package eu.dnetlib.iis.wf.importer.content;

/**
 * Repository URLs
 * @author mhorst
 *
 */
public class RepositoryUrls {

	private final String repositoryId;
	
	private final String[] urls;
	
	public RepositoryUrls(String repositoryId, String[] urls) {
		this.repositoryId = repositoryId;
		this.urls = urls;
	}

	public String getRepositoryId() {
		return repositoryId;
	}

	public String[] getUrls() {
		return urls;
	}
	
}
