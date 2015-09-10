package eu.dnetlib.iis.importer.content;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * HDFS files based content provider.
 * This implementation is not thread-safe and is intended for testing purposes.
 * Reads pdf files from hdfs. As an input expects path to index file containing
 * properties pairs: result_id = pdf_file_location where pdf_file_location is the
 * relative location to the directory holding index file.
 *  
 * @author mhorst
 *
 */
public class HdfsFilesContentProviderService implements ContentProviderService {

	private final Logger log = Logger.getLogger(HdfsFilesContentProviderService.class);
	
	private FileSystem fs;
	
	private Path indexFilePath;
	
	/**
	 * Cached ids pointing to pdf files.
	 */
	private Map<String,Path> cachedIdsMap;
	
	/**
	 * Default constructor.
	 * @param context
	 * @param indexFilePath
	 * @throws IOException 
	 */
	public HdfsFilesContentProviderService(
			Configuration configuration,
			Path indexFilePath) throws IOException {
		this.fs = FileSystem.get(configuration);
		this.indexFilePath = indexFilePath;
	}
	
	@Override
	public byte[] getContent(String id,
			RepositoryUrls[] repositoryUrls) throws IOException {
		if (cachedIdsMap==null) {
//			initializing, collecting all ids and files paths
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(indexFilePath)));
			try {
				Properties properties = new Properties();
				properties.load(br);
//				setting absolute paths
				cachedIdsMap = new HashMap<String, Path>();
				Path rootPath = indexFilePath.getParent();
				for (Entry<Object, Object> entry : properties.entrySet()) {
//					TODO remove this message
					log.error("storing path for id " + entry.getKey() + 
							", path: " + new Path(rootPath, entry.getValue().toString()));
					
					cachedIdsMap.put(entry.getKey().toString(), 
							new Path(rootPath, entry.getValue().toString()));
				}
				
			} finally {
				br.close();
			}
		}
		if (cachedIdsMap.containsKey(id)) {
//			TODO change log level
			Path path = cachedIdsMap.get(id);
			log.error("returning content for id: " + id + 
					", path: " + path);
			FSDataInputStream inputStream = fs.open(path);
			try {
				return IOUtils.toByteArray(	inputStream);	
			} finally {
				inputStream.close();
			}
		} else {
			log.debug("content not available for id " + id);
		}
//		fallback
		return null;
	} 
}
