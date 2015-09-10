package eu.dnetlib.iis.importer.dataset;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.commons.io.IOUtils;
import org.springframework.util.StringUtils;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreFile;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.importer.content.ObjectStoreContentProviderUtils;

public class ObjectStoreDataDumpProvider {

	public static void main(String[] args) throws Exception {
		String objectStoreLocation = "http://openaire-services.vls.icm.edu.pl:8280/is/services/objectStore";
//		arxiv
//		String currentObjectStoreId = "258755af-0b48-41ee-9652-939c5bd2fca3_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		puma
//		String currentObjectStoreId = "794e8173-8be3-4f51-a12e-b43d12ab3b7d_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		WoS
//		String currentObjectStoreId = "a1a35f9d-dc12-44e0-8781-d8273f5ef017_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		pubmed
		String currentObjectStoreId = "b2b6fca5-ce18-498c-a375-b02df97998f0_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";

//		short version required when costructing URL manually
//		String currentObjectStoreId = "b2b6fca5-ce18-498c-a375-b02df97998f0";
		
		String objectIdsCSV = args[0];
		
		String[] objectIds = StringUtils.delimitedListToStringArray(objectIdsCSV, ",");
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreLocation);
		eprBuilder.build();
		ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(ObjectStoreService.class, eprBuilder.build());
		
		String pmid = "16509961";
		File targetDir = new File("/home/azio/Downloads/pmc-records/" + pmid);
		targetDir.mkdirs();
		
		for (String currentObjectId : objectIds) {
			FileOutputStream fos = new FileOutputStream(new File(targetDir, currentObjectId + ".xml"));
			try {
				String objStoreJson = objectStore.deliverRecord(
						currentObjectStoreId, currentObjectId);
				System.out.println(objStoreJson);
				ObjectStoreFile objStoreFile = ObjectStoreFile.createObject(
						objStoreJson);
						
				byte[] result = ObjectStoreContentProviderUtils.getContentFromURL(
						objStoreFile.getURI(),
						60000, 60000);
				IOUtils.write(result, fos);				
				
			} finally {
				fos.close();	
			}
		}
	}
}
