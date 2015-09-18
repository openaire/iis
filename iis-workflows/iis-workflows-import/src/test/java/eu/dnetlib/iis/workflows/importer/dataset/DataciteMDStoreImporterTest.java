package eu.dnetlib.iis.workflows.importer.dataset;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.ws.wsaddressing.W3CEndpointReference;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Test;
import org.xml.sax.InputSource;

import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreFile;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.enabling.database.rmi.DatabaseService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.workflows.importer.content.ObjectStoreContentProviderUtils;
import eu.dnetlib.iis.workflows.importer.database.project.DatabaseProjectXmlHandler;
import eu.dnetlib.iis.workflows.importer.dataset.RecordReceiver;

/**
 * Datacite MDStore importer test.
 * @author mhorst
 *
 */
public class DataciteMDStoreImporterTest {

	Logger log = Logger.getLogger(this.getClass()); 
	
//	@Test
	public void testRelationalDatabaseAccess() throws Exception {
//		old API
//		String databaseServiceLocation = "http://node0.t.openaire.research-infrastructures.eu:8080/is/services/database";
//		String databaseServiceLocation = "http://node1.t.openaire.research-infrastructures.eu:8280/is/services/database";
//		new API
		String databaseServiceLocation = "http://node6.t.openaire.research-infrastructures.eu:8280/is/services/database";
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(databaseServiceLocation);
		eprBuilder.build();
		DatabaseService databaseService = new JaxwsServiceResolverImpl().getService(DatabaseService.class, eprBuilder.build());
		System.out.println(databaseService.identify());
		
//		String db = "dnet_openaireplus";
		String db = "dnet_openaireplus_node6_t";
		
		InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream("eu/dnetlib/iis/workflows/importer/database/project/sql/read_project_details.sql");
		StringWriter writer = new StringWriter();
		IOUtils.copy(in, writer, "utf-8");
		String sql = writer.toString();
		
		W3CEndpointReference results = databaseService.searchSQL(db, sql);
		
		ResultSetClientFactory rsFactory = new ResultSetClientFactory();
		rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
		rsFactory.setPageSize(10);
		
		SAXParserFactory parserFactory = SAXParserFactory.newInstance();
		SAXParser saxParser = parserFactory.newSAXParser();
		
		int maxResults = 1;
		int currentIdx=0;
		for (String result : rsFactory.getClient(results)) {
//			System.out.println(result);
			saxParser.parse(new InputSource(new StringReader(result)),
					new DatabaseProjectXmlHandler(new RecordReceiver<Project>() {
						@Override
						public void receive(Project object) throws IOException {
							System.out.println("id: " + object.getId());
//							System.out.println("acronym: " + object.getProjectAcronym());
//							System.out.println("grantid: " + object.getProjectGrantId());
							System.out.println("fundingclass: " + object.getFundingClass());
						}
					}));
			System.out.println(result);
			currentIdx++;
			if (currentIdx>maxResults) {
				break;
			}
		}
	}
	
//	@Test
	public void testObjectStoreAccess() throws Exception {
//		old API
//		String objectStoreLocation = "http://node0.t.openaire.research-infrastructures.eu:8080/is/services/objectStore";
//		new API
		String objectStoreLocation = "http://node6.t.openaire.research-infrastructures.eu:8280/is/services/objectStore";
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreLocation);
		eprBuilder.build();
		ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(ObjectStoreService.class, eprBuilder.build());
		System.out.println(objectStore.identify());
		
		for (String currentObjectStore : objectStore.getListOfObjectStores()) {
			System.out.println(currentObjectStore);
		}
		
//		arxiv
//		String currentObjectStoreId = "258755af-0b48-41ee-9652-939c5bd2fca3_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		puma
		String currentObjectStoreId = "794e8173-8be3-4f51-a12e-b43d12ab3b7d_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		WoS
//		String currentObjectStoreId = "a1a35f9d-dc12-44e0-8781-d8273f5ef017_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		pubmed
//		String currentObjectStoreId = "b2b6fca5-ce18-498c-a375-b02df97998f0_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
		
		List<String> mimeTypesPdf = new ArrayList<String>();
		mimeTypesPdf.add("application/pdf");
		mimeTypesPdf.add("pdf");
		List<String> mimeTypesText = new ArrayList<String>();
		mimeTypesPdf.add("text/plain");
		
		log.warn("starting importing process from object store: " + currentObjectStoreId);
		W3CEndpointReference objStoreResults = objectStore.deliverObjects(
				currentObjectStoreId, 
				0l,
                System.currentTimeMillis());
		log.warn("obtained ObjectStore ResultSet EPR: " + objStoreResults.toString());

//		obtaining resultSet
		
		ResultSetClientFactory rsFactory = new ResultSetClientFactory();
		rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
		rsFactory.setPageSize(10);
		
		for (String record : rsFactory.getClient(objStoreResults)) {
			System.out.println(record);
			ObjectStoreFile objStoreFile = ObjectStoreFile.createObject(record);
			if (objStoreFile!=null) {
				String resultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(
						objStoreFile.getObjectID());
				String resourceLoc = objStoreFile.getURI();
				System.out.println("generated id:" + resultId);
//				if (mimeTypesPdf.contains(objStoreFile.getMimeType())) {
////					storing as content
//					long startTimeContent = System.currentTimeMillis();
//					byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(
//							resourceLoc, 2000, 2000);
//					log.warn("content retrieval for id: " + objStoreFile.getObjectID() + 
//							" and location: " + resourceLoc + " took: " +
//							(System.currentTimeMillis()-startTimeContent)/1000 + " secs, got content: " +
//							(content!=null && content.length>0));
//				} else if (mimeTypesText.contains(objStoreFile.getMimeType())) {
////					storing as text
//					long startTimeContent = System.currentTimeMillis();
//					byte[] textContent = ObjectStoreContentProviderUtils.getContentFromURL(
//							resourceLoc, 2000, 2000);
//					log.warn("text content retrieval for id: " + objStoreFile.getObjectID() + 
//							" and location: " + resourceLoc + " took: " +
//							(System.currentTimeMillis()-startTimeContent)/1000 + " secs, got text content: " +
//							(textContent!=null && textContent.length>0));
//					
//					log.warn("text content " + objStoreFile.getObjectID() + " retrieved successfully " +
//							"for location: " + resourceLoc);
//				} else {
//					log.warn("got unhandled mime type: " + objStoreFile.getMimeType() + 
//							" for object: " + objStoreFile.getObjectID());
//				}
			}
			
			
		}
		log.warn("importing process from object store: " + currentObjectStoreId + " has finished");
		
		
	}
	
//	@Test
	public void testObjectStoreRecordAccess() throws Exception {
//		new API
		String objectStoreLocation = "http://node6.t.openaire.research-infrastructures.eu:8280/is/services/objectStore";
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreLocation);
		eprBuilder.build();
		ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(ObjectStoreService.class, eprBuilder.build());
		System.out.println(objectStore.identify());
		
		for (String currentObjectStore : objectStore.getListOfObjectStores()) {
			System.out.println(currentObjectStore);
		}
		
//		arxiv
//		String currentObjectStoreId = "258755af-0b48-41ee-9652-939c5bd2fca3_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		puma
//		String currentObjectStoreId = "794e8173-8be3-4f51-a12e-b43d12ab3b7d_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		WoS
//		String currentObjectStoreId = "a1a35f9d-dc12-44e0-8781-d8273f5ef017_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
//		pubmed
		String currentObjectStoreId = "b2b6fca5-ce18-498c-a375-b02df97998f0_T2JqZWN0U3RvcmVEU1Jlc291cmNlcy9PYmplY3RTdG9yZURTUmVzb3VyY2VUeXBl";
		
		List<String> mimeTypesPdf = new ArrayList<String>();
		mimeTypesPdf.add("application/pdf");
		mimeTypesPdf.add("pdf");
		List<String> mimeTypesText = new ArrayList<String>();
		mimeTypesPdf.add("text/plain");
		
		String record = objectStore.deliverRecord(currentObjectStoreId, 
				"od_______908::8ee2020510832afa7252599132c59497");
		System.out.println("got record: " + record);
		
		
		
	}

//	@Test
//	manual test
	public void testMDStoreRecordAccess() throws Exception {
//		datacite
		String mdStoreId = "6af45361-c587-4486-b7c3-ee9e5b202037_TURTdG9yZURTUmVzb3VyY2VzL01EU3RvcmVEU1Jlc291cmNlVHlwZQ==";
//		String mdStoreId = "452697fc-a3d5-4909-97db-0634fd1dbe7b_TURTdG9yZURTUmVzb3VyY2VzL01EU3RvcmVEU1Jlc291cmNlVHlwZQ==";
//		String mdStoreId = "f8193d4e-e75e-4b59-bd8b-50760c156399_TURTdG9yZURTUmVzb3VyY2VzL01EU3RvcmVEU1Jlc291cmNlVHlwZQ==";
//		String mdStoreId = "a55e0bd5-cc32-4a30-98d2-7036611aeb39_TURTdG9yZURTUmVzb3VyY2VzL01EU3RvcmVEU1Jlc291cmNlVHlwZQ==";
			
		String mdStoretLocation = "http://beta.services.openaire.eu:8280/is/services/mdStore";
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(mdStoretLocation);
		eprBuilder.build();
		MDStoreService mdStore = new JaxwsServiceResolverImpl().getService(MDStoreService.class, eprBuilder.build());
		System.out.println(mdStore.identify());
		try {
		System.out.println(mdStore.deliverRecord(
				mdStoreId, 
//				"datacite____::000025f1f3f940fdf71bbd80b7d7f6f9"));
				"r39633d1e8c4::145f7b81f966de31148af86aaa63d40c"));
		} catch (DocumentNotFoundException e) {
			System.out.println("document not found!");
			throw e;
		} catch (Exception e) {
			System.out.println(e.getClass().getName());
			throw e;
		}
	}
}
