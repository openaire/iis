package eu.dnetlib.iis.wf.importer.dataset;

import static org.junit.Assert.assertEquals;
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
			saxParser = SAXParserFactory.newInstance().newSAXParser();
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
			assertEquals("50|oai:oai.datacite.org:1805127", receivedReferences.get(0).getId());
			assertEquals("10.6068/DP13F04CCEE7995", receivedReferences.get(0).getIdForGivenType());
			assertEquals(1, receivedReferences.get(0).getCreatorNames().size());
			assertEquals("creator1", receivedReferences.get(0).getCreatorNames().get(0));
			assertEquals(1, receivedReferences.get(0).getTitles().size());
			assertEquals("title1", receivedReferences.get(0).getTitles().get(0));
			assertEquals(1, receivedReferences.get(0).getFormats().size());
			assertEquals("format1", receivedReferences.get(0).getFormats().get(0));
			assertEquals("some description", receivedReferences.get(0).getDescription());
			assertEquals("publisher1", receivedReferences.get(0).getPublisher());
			assertEquals("2012", receivedReferences.get(0).getPublicationYear());
			assertEquals("Text", receivedReferences.get(0).getResourceTypeClass());
			assertNull(receivedReferences.get(0).getResourceTypeValue());
//			2nd record
			assertEquals("10.6068/DP13F04CD013D96", receivedReferences.get(1).getIdForGivenType());
			assertEquals(1, receivedReferences.get(1).getCreatorNames().size());
			assertEquals("creator2", receivedReferences.get(1).getCreatorNames().get(0));
			assertEquals(1, receivedReferences.get(1).getTitles().size());
			assertEquals("title2", receivedReferences.get(1).getTitles().get(0));
			assertNull(receivedReferences.get(1).getFormats());
			assertEquals("publisher2", receivedReferences.get(1).getPublisher());
			assertEquals("2013", receivedReferences.get(1).getPublicationYear());
			assertEquals("Dataset", receivedReferences.get(1).getResourceTypeClass());
			assertEquals("Dataset", receivedReferences.get(1).getResourceTypeValue());
			
		} finally {
			if (inputStream!=null) {
				inputStream.close();
			}	
		}
	}
	
}
