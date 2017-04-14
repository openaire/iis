package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.content.DocumentContentUrlDispatcher.PROPERTY_MULTIPLEOUTPUTS;
import static eu.dnetlib.iis.wf.importer.content.DocumentContentUrlDispatcher.PROPERTY_PREFIX_MIMETYPES_CSV;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class DocumentContentUrlDispatcherTest {

    @Mock
    private Context context;
    
    @Mock
    private MultipleOutputs multipleOutputs;

    @Captor
    private ArgumentCaptor<String> mosKeyCaptor;
    
    @Captor
    private ArgumentCaptor<AvroKey<?>> mosValueCaptor;
    
    
    private DocumentContentUrlDispatcher dispatcher;

    
    @Before
    public void init() throws Exception {
        dispatcher = new DocumentContentUrlDispatcher() {
            
            @Override
            protected MultipleOutputs instantiateMultipleOutputs(Context context) {
                return multipleOutputs;
            }
            
        };
    }
    
    // ------------------------------------- TESTS -----------------------------------
    
    @Test(expected=IllegalArgumentException.class)
    public void testSetupNoParams() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        dispatcher.setup(context);
    }


    @Test
    public void testDispatch() throws Exception {
        // given
        String mimeTypePdf = "pdf,application/pdf";
        String mimeTypeHtml = "text/html";
        String outputNamePdf = "pdf";
        String outputNameHtml = "html";
        Configuration conf = new Configuration();
        conf.set(PROPERTY_MULTIPLEOUTPUTS, buildMultipleOutputsProperty(outputNamePdf, outputNameHtml));
        conf.set(PROPERTY_PREFIX_MIMETYPES_CSV + outputNamePdf, mimeTypePdf);
        conf.set(PROPERTY_PREFIX_MIMETYPES_CSV + outputNameHtml, mimeTypeHtml);
        doReturn(conf).when(context).getConfiguration();
        dispatcher.setup(context);
        
        DocumentContentUrl pdfUrl = buildDocumentContentUrl("pdfId", "pdf");
        DocumentContentUrl htmlUrl = buildDocumentContentUrl("htmlId", mimeTypeHtml);
        
        // execute
        dispatcher.map(new AvroKey<>(pdfUrl), null, context);
        dispatcher.map(new AvroKey<>(htmlUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(2)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        assertEquals(outputNamePdf, mosKeyCaptor.getAllValues().get(0));
        DocumentContentUrl obtainedPdfUrl = (DocumentContentUrl) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(obtainedPdfUrl);
        assertTrue(pdfUrl == obtainedPdfUrl);
        assertEquals(outputNameHtml, mosKeyCaptor.getAllValues().get(1));
        DocumentContentUrl obtainedHtmlUrl = (DocumentContentUrl) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(obtainedHtmlUrl);
        assertTrue(htmlUrl == obtainedHtmlUrl);
    }
    
    @Test
    public void testDispatchForUndefinedMimeType() throws Exception {
        // given
        String mimeTypeHtml = "text/html";
        String outputNameHtml = "html";
        Configuration conf = new Configuration();
        conf.set(PROPERTY_MULTIPLEOUTPUTS, buildMultipleOutputsProperty(outputNameHtml));
        conf.set(PROPERTY_PREFIX_MIMETYPES_CSV + outputNameHtml, mimeTypeHtml);
        doReturn(conf).when(context).getConfiguration();
        dispatcher.setup(context);
        
        DocumentContentUrl htmlUrl = buildDocumentContentUrl("htmlId", null);

        
        // execute
        dispatcher.map(new AvroKey<>(htmlUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testDispatchForUnsupportedMimeType() throws Exception {
        // given
        String mimeTypePdf = "pdf,application/pdf";
        String outputNamePdf = "pdf";
        Configuration conf = new Configuration();
        conf.set(PROPERTY_MULTIPLEOUTPUTS, buildMultipleOutputsProperty(outputNamePdf));
        conf.set(PROPERTY_PREFIX_MIMETYPES_CSV + outputNamePdf, mimeTypePdf);
        doReturn(conf).when(context).getConfiguration();
        dispatcher.setup(context);
        
        DocumentContentUrl xmlUrl = buildDocumentContentUrl("xmlId", "xml");
        
        // execute
        dispatcher.map(new AvroKey<>(xmlUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
    }
    
    @Test
    public void testCleanup() throws Exception {
        // given
        String outputNamePdf = "pdf";
        Configuration conf = new Configuration();
        conf.set(PROPERTY_MULTIPLEOUTPUTS, buildMultipleOutputsProperty(outputNamePdf));
        doReturn(conf).when(context).getConfiguration();
        dispatcher.setup(context);
        
        // execute
        dispatcher.cleanup(context);
        
        // assert
        verify(multipleOutputs, times(1)).close();
    }
    
    // ---------------------------------- PRIVATE ------------------------------------
    
    private static String buildMultipleOutputsProperty(String... outputNames) {
        StringBuilder strBuilder = new StringBuilder();
        for (int i=0; i < outputNames.length; i++) {
            strBuilder.append(outputNames[i]);
            if (i < outputNames.length - 1) {
                strBuilder.append(' ');    
            }
        }
        return strBuilder.toString();
    }
    
    private static DocumentContentUrl buildDocumentContentUrl(String id, String mimeType) {
        DocumentContentUrl.Builder docContentUrlBuilder = DocumentContentUrl.newBuilder();
        docContentUrlBuilder.setId(id);
        docContentUrlBuilder.setUrl("docUrl");
        docContentUrlBuilder.setMimeType(mimeType);
        docContentUrlBuilder.setContentSizeKB(1l);
        return docContentUrlBuilder.build();
    }
    
    
}
