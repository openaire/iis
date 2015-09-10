package eu.dnetlib.iis.importer.content;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.log4j.Logger;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreServiceException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.importer.content.appover.ComplexContentApprover;
import eu.dnetlib.iis.importer.content.appover.ContentApprover;
import eu.dnetlib.iis.importer.content.appover.PDFHeaderBasedContentApprover;
import eu.dnetlib.iis.importer.content.appover.SizeLimitContentApprover;

/**
 * {@link ObjectStoreService} based content provider.
 * @author mhorst
 *
 */
public class ObjectStoreContentProviderService implements
		ContentProviderService {

	private final Logger log = Logger.getLogger(this.getClass());
	
	private static final String digestAlgorithm = "MD5";
	
	
	/**
	 * Object store service.
	 */
	private final ObjectStoreService objectStore;
	
	/**
	 * Lookup service location.
	 */
	private final ISLookUpService lookupService;
	
	/**
	 * Connection timeout when reading content.
	 */
	private final int connectionTimeout;
	
	/**
	 * Read timeout when reading content.
	 */
	private final int readTimeout;
	
	/**
	 * Cache holding repository identifier to object store identifier mappings.
	 */
	private final Map<String, String> repositoryToObjectStoreCache = new HashMap<String, String>();
	
	/**
	 * Content approver module.
	 */
	private final ContentApprover contentApprover;
	
	/**
	 * Default constructor.
	 * @param objectStoreServiceLocation
	 * @param lookupServiceLocation
	 * @param connectionTimeout
	 * @param readTimeout
	 * @param sizeLimitMegabytes
	 */
	public ObjectStoreContentProviderService(
			String objectStoreServiceLocation,
			String lookupServiceLocation,
			int connectionTimeout, int readTimeout,
			int sizeLimitMegabytes) {
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreServiceLocation);
		eprBuilder.build();
		this.objectStore = new JaxwsServiceResolverImpl().getService(
				ObjectStoreService.class, eprBuilder.build());
		eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(lookupServiceLocation);
		eprBuilder.build();
		this.lookupService = new JaxwsServiceResolverImpl().getService(
				ISLookUpService.class, eprBuilder.build());
		this.connectionTimeout = connectionTimeout;
		this.readTimeout = readTimeout;
		if (sizeLimitMegabytes > 0) {
			this.contentApprover = new ComplexContentApprover(
					new PDFHeaderBasedContentApprover(),
					new SizeLimitContentApprover(sizeLimitMegabytes));
		} else {
			this.contentApprover = new PDFHeaderBasedContentApprover();	
		}
	}
	
	@Override
	public byte[] getContent(String id,
			RepositoryUrls[] repositoryUrls) throws IOException {
		if (repositoryUrls!=null && repositoryUrls.length>0) {
			for (RepositoryUrls repositoryUrl : repositoryUrls) {
				String objectStoreId = provideObjectStoreId(repositoryUrl.getRepositoryId());
				if (objectStoreId!=null) {
					for (String currentUrl : repositoryUrl.getUrls()) {
						String generatedObjectId = ObjectStoreContentProviderUtils.generateObjectId(
								id, 
								currentUrl, 
								ObjectStoreContentProviderUtils.defaultEncoding, 
								digestAlgorithm);
						try {
							String contentURLLoc = objectStore.deliverRecord(
									objectStoreId, generatedObjectId);
							if (contentURLLoc!=null) {
								byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(
										contentURLLoc,
										connectionTimeout, readTimeout);
								if (contentApprover.approve(result)) {
									return result;
								}
							}
						} catch (ObjectStoreServiceException e) {
//							TODO check whether non existing object causes throwing similar exception
							throw new IOException("unable to retrieve object " + objectStoreId + 
									" from object store id: " + objectStoreId + 
									". Object identifier was generated from resultid: " + id + 
									" and url: " + currentUrl, e);
						}
					}
				}
			}
		} else {
			log.warn("got no repository URLs along with record id " + id + 
					", no contents will be retrieved");
		}
//		fallback
		return null;
	}
	
	/**
	 * Provides object store identifier using local cache.
	 * @param repositoryId
	 * @return object store identifier or null when no corresponding object store was found for repository.
	 */
	private String provideObjectStoreId(String repositoryId) {
		if (repositoryId==null) {
			return null;
		}
		if (repositoryToObjectStoreCache.containsKey(repositoryId)) {
			return repositoryToObjectStoreCache.get(repositoryId);
		} else {
			String objectStoreId = ObjectStoreContentProviderUtils.objectStoreIdLookup(
					lookupService, repositoryId);
			repositoryToObjectStoreCache.put(repositoryId, objectStoreId);
			return objectStoreId;
		}
	}

}
