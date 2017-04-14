package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;



/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ObjectStoreDocumentContentUrlImporterMapperTest {

    private final static String EXPECTED_OUTPUT_ROOT_LOCATION = "eu/dnetlib/iis/wf/importer/content/output/";
    
    @Mock
    private Context context;
    
    @Captor
    private ArgumentCaptor<AvroKey<DocumentContentUrl>> contentUrlCaptor;
    
    
    private ObjectStoreDocumentContentUrlImporterMapper mapper = new ObjectStoreDocumentContentUrlImporterMapper();

    // ------------------------------------- TESTS -----------------------------------
    
    @Test(expected=RuntimeException.class)
    public void testSetupWithoutISLookupFacade() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test(expected=IOException.class)
    public void testMapThrowingException() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.content.ExceptionThrowingObjectStoreFacadeFactory");
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        
        AvroKey<Identifier> key = new AvroKey<>(buildIdentifier("objectStoreId"));
        
        // execute
        mapper.map(key, null, context);
    }
    
    @Test
    public void testMap() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.content.MockObjectStoreFacadeFactory");
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        
        AvroKey<Identifier> key = new AvroKey<>(buildIdentifier("objectStoreId"));
        
        // execute
        mapper.map(key, null, context);
        
        // assert
        int expectedCount = 5;
        verify(context, times(expectedCount)).write(contentUrlCaptor.capture(), any());
        List<DocumentContentUrl> expectedList = loadExpected();
        for (int i=0; i < expectedCount; i++) {
            assertEquals(expectedList.get(i), contentUrlCaptor.getAllValues().get(i).datum());
        }
    }

    
    // ------------------------------------- PRIVATE -----------------------------------

    private Identifier buildIdentifier(String id) {
        return Identifier.newBuilder().setId(id).build();
    }
    
    private List<DocumentContentUrl> loadExpected() {
        List<DocumentContentUrl> results = new ArrayList<>();
        results.addAll(loadExpected("document_content_url_html.json"));
        results.addAll(loadExpected("document_content_url_pdf.json"));
        results.addAll(loadExpected("document_content_url_pdf2.json"));
        results.addAll(loadExpected("document_content_url_wos.json"));
        results.addAll(loadExpected("document_content_url_xml.json"));
        return results;
    }
    
    private List<DocumentContentUrl> loadExpected(String fileName) {
        return JsonUtils.convertToList(EXPECTED_OUTPUT_ROOT_LOCATION + fileName, 
                DocumentContentUrl.SCHEMA$, DocumentContentUrl.class);
    }
}
