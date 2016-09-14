package eu.dnetlib.iis.wf.importer.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

/**
 * {@link DataciteXmlImporter} test class.
 * @author mhorst
 *
 */
public class DataciteXmlImporterTest {

	@Test
	public void testDataciteImport() throws Exception {
		String filePath = "/eu/dnetlib/iis/wf/importer/dataset/data/input/datacite_test_dump.xml";
		String mdStoreId = "some-mdstore-id";
		InputStream inputStream = null;
		SAXParser saxParser = null;
		final List<DataSetReference> receivedReferences = new ArrayList<DataSetReference>();
		final List<DatasetToMDStore> receivedMDStoreReferences = new ArrayList<DatasetToMDStore>();
		try {
		    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		    saxParserFactory.setNamespaceAware(true);
			saxParser = saxParserFactory.newSAXParser();
			DataciteDumpXmlHandler handler = new DataciteDumpXmlHandler(new RecordReceiver<DataSetReference>() {
				@Override
				public void receive(DataSetReference object) throws IOException {
					receivedReferences.add(object);
				}
			},new RecordReceiver<DatasetToMDStore>() {
				@Override
				public void receive(DatasetToMDStore object) throws IOException {
					receivedMDStoreReferences.add(object);
				}
			}, mdStoreId); 
			saxParser.parse(inputStream = DataciteXmlImporterTest.class.getResourceAsStream(filePath),
					handler , DataciteDumpXmlHandler.ELEM_OBJ_IDENTIFIER);
			
			assertEquals(2, receivedReferences.size());
			assertEquals(2, receivedMDStoreReferences.size());
			
//			checking mdstore references
			assertEquals("50|oai:oai.datacite.org:1805127", receivedMDStoreReferences.get(0).getDatasetId());
			assertEquals(mdStoreId, receivedMDStoreReferences.get(0).getMdStoreId());
			assertEquals("50|oai:oai.datacite.org:1805128", receivedMDStoreReferences.get(1).getDatasetId());
			assertEquals(mdStoreId, receivedMDStoreReferences.get(1).getMdStoreId());
			
//			1st record
			DataSetReference currentReference = receivedReferences.get(0);
			assertNotNull(currentReference);
			assertEquals("50|oai:oai.datacite.org:1805127", currentReference.getId());
			assertEquals("10.6068/DP13F04CCEE7995", currentReference.getIdForGivenType());
			assertEquals(1, currentReference.getCreatorNames().size());
			assertEquals("creator1", currentReference.getCreatorNames().get(0));
			assertEquals(1, currentReference.getTitles().size());
			assertEquals("title1", currentReference.getTitles().get(0));
			assertEquals(1, currentReference.getFormats().size());
			assertEquals("format1", currentReference.getFormats().get(0));
			assertEquals("some description", currentReference.getDescription());
			assertEquals("publisher1", currentReference.getPublisher());
			assertEquals("2012", currentReference.getPublicationYear());
			assertEquals("Text", currentReference.getResourceTypeClass());
			assertNull(currentReference.getResourceTypeValue());
			assertNotNull(currentReference.getAlternateIdentifiers());
            assertEquals(2, currentReference.getAlternateIdentifiers().size());
            assertEquals("AAA0012345", currentReference.getAlternateIdentifiers().get("firstType"));
            assertEquals("BBB0012345", currentReference.getAlternateIdentifiers().get("secondType"));

			
//			2nd record
            currentReference = receivedReferences.get(1);
            assertNotNull(currentReference);
			assertEquals("10.6068/DP13F04CD013D96", currentReference.getIdForGivenType());
			assertEquals(1, currentReference.getCreatorNames().size());
			assertEquals("creator2", currentReference.getCreatorNames().get(0));
			assertEquals(1, currentReference.getTitles().size());
			assertEquals("title2", currentReference.getTitles().get(0));
			assertNull(currentReference.getFormats());
			assertEquals("publisher2", currentReference.getPublisher());
			assertEquals("2013", currentReference.getPublicationYear());
			assertEquals("Dataset", currentReference.getResourceTypeClass());
			assertEquals("Dataset", currentReference.getResourceTypeValue());
			assertNull(currentReference.getAlternateIdentifiers());
						
		} finally {
			if (inputStream!=null) {
				inputStream.close();
			}	
		}
	}
	
}
