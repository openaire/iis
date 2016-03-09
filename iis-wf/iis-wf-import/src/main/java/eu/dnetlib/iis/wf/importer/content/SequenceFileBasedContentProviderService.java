package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.importer.schemas.DocumentContent;

/**
 * Sequence file based content provider.
 * This implementation is not thread-safe and is intended for testing purposes only
 * as reading from sequence file is unefficient. 
 * @author mhorst
 *
 */
public class SequenceFileBasedContentProviderService implements ContentProviderService {

	private final Logger log = Logger.getLogger(SequenceFileBasedContentProviderService.class);
	
	private Configuration conf;
	
	private Path datastorePath;
	
	private Set<String> cachedIds;
	
	/**
	 * Default constructor.
	 * @param context
	 * @param datastorePath
	 */
	public SequenceFileBasedContentProviderService(
			Configuration conf, Path datastorePath) {
		this.conf = conf;
		this.datastorePath = datastorePath;
	}
	
	@Override
	public byte[] getContent(String id,
			RepositoryUrls[] repositoryUrls) throws IOException {
		if (cachedIds!=null) {
			if (cachedIds.contains(id)) {
				CloseableIterator<DocumentContent> it = DataStore.getReader(
						new FileSystemPath(
								FileSystem.get(this.conf), 
								this.datastorePath)); 
				try {
					while(it.hasNext()) {
						DocumentContent content = it.next();
						if (id.equals(content.getId()) &&
								content.getPdf()!=null) {
							return content.getPdf().array();
						}
					}	
				} finally {
					it.close();
				}
			} else {
				log.debug("content not available for id " + id);
			}
//			fallback
			return null;
		} else {
//			first run mode, collecting all ids
			CloseableIterator<DocumentContent> it = DataStore.getReader(
					new FileSystemPath(FileSystem.get(this.conf), 
							this.datastorePath));
			Set<String> ids = new HashSet<String>();
			byte[] result = null;
			try {
				while(it.hasNext()) {
					DocumentContent content = it.next();
					ids.add(content.getId().toString());
					if (id.equals(content.getId()) &&
							content.getPdf()!=null) {
						result = content.getPdf().array();
					}
				}	
			} finally {
				it.close();
			}
			cachedIds = ids;
			return result;
		}
	}

}
