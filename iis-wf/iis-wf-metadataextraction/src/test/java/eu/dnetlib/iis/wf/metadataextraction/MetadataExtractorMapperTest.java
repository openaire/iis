package eu.dnetlib.iis.wf.metadataextraction;

import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.EXCLUDED_IDS;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.INTERRUPT_PROCESSING_TIME_THRESHOLD_SECS;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.LOG_FAULT_PROCESSING_TIME_THRESHOLD_SECS;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.NAMED_OUTPUT_FAULT;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.NAMED_OUTPUT_META;
import static eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMapper.InvalidRecordCounters.INVALID_PDF_HEADER;
import static eu.dnetlib.iis.wf.metadataextraction.NlmToDocumentWithBasicMetadataConverter.EMPTY_META;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.itextpdf.text.exceptions.InvalidPdfException;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import pl.edu.icm.cermine.tools.timeout.TimeoutException;


/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class MetadataExtractorMapperTest {

    private static final String PDF_FILE = "/eu/dnetlib/iis/wf/metadataextraction/pdf-example.pdf";
    
    private static final String NON_PDF_FILE = "/eu/dnetlib/iis/wf/metadataextraction/nlm-example.xml";
    
    @Mock
    private Context context;
    
    @Mock
    private MultipleOutputs multipleOutputs;
    
    @Captor
    private ArgumentCaptor<String> mosKeyCaptor;
    
    @Captor
    private ArgumentCaptor<AvroKey<?>> mosValueCaptor;
    
    @Mock
    private Counter invalidPdfCounter;
    
    
    private MetadataExtractorMapper mapper;

    
    @Before
    public void init() throws Exception {
        mapper = new MetadataExtractorMapper() {
            
            @Override
            protected MultipleOutputs instantiateMultipleOutputs(Context context) {
                return multipleOutputs;
            }
            
        };
    }
    
    // ------------------------------------- TESTS -----------------------------------
    
    @Test(expected=RuntimeException.class)
    public void testSetupWithoutNamedOutputMeta() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test(expected=RuntimeException.class)
    public void testSetupWithoutNamedOutputFault() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test
    public void testMap() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        docContentBuilder.setPdf(ByteBuffer.wrap(getContent(PDF_FILE)));
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(1)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(NAMED_OUTPUT_META), mosKeyCaptor.getValue());
        ExtractedDocumentMetadata docMeta = (ExtractedDocumentMetadata) mosValueCaptor.getValue().datum();
        assertNotNull(docMeta);
        assertEquals(id, docMeta.getId());
        
        verify(invalidPdfCounter, never()).increment(1);
    }
    
    @Test
    public void testMapWithExcludedIds() throws Exception {
        // given
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        docContentBuilder.setPdf(ByteBuffer.wrap(getContent(PDF_FILE)));
        
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        conf.set(EXCLUDED_IDS, id);
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
        verify(invalidPdfCounter, never()).increment(1);
    }
    
    @Test
    public void testMapWithNullContent() throws Exception {
        // given
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, never()).write(any(), any());
        verify(invalidPdfCounter, never()).increment(1);
    }
    
    @Test
    public void testMapWithIvalidPdf() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        docContentBuilder.setPdf(ByteBuffer.wrap(getContent(NON_PDF_FILE)));
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(2)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(NAMED_OUTPUT_META), mosKeyCaptor.getAllValues().get(0));
        ExtractedDocumentMetadata docMeta = (ExtractedDocumentMetadata) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(docMeta);
        assertEquals(id, docMeta.getId());
        assertEquals("", docMeta.getText());
        assertEquals(EMPTY_META, docMeta.getPublicationTypeName());
        // fault
        assertEquals(conf.get(NAMED_OUTPUT_FAULT), mosKeyCaptor.getAllValues().get(1));
        Fault fault = (Fault) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(fault);
        assertEquals(id, fault.getInputObjectId());
        assertEquals(InvalidPdfException.class.getName(), fault.getCode());
        assertTrue(fault.getTimestamp() > 0);
        
        verify(invalidPdfCounter, times(1)).increment(1);
    }
    
    @Test
    public void testMapWithInterruption() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        conf.set(INTERRUPT_PROCESSING_TIME_THRESHOLD_SECS, String.valueOf(1));
        
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        docContentBuilder.setPdf(ByteBuffer.wrap(getContent(PDF_FILE)));
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(2)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(NAMED_OUTPUT_META), mosKeyCaptor.getAllValues().get(0));
        ExtractedDocumentMetadata docMeta = (ExtractedDocumentMetadata) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(docMeta);
        assertEquals(id, docMeta.getId());
        assertEquals("", docMeta.getText());
        assertEquals(EMPTY_META, docMeta.getPublicationTypeName());
        // fault
        assertEquals(conf.get(NAMED_OUTPUT_FAULT), mosKeyCaptor.getAllValues().get(1));
        Fault fault = (Fault) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(fault);
        assertEquals(id, fault.getInputObjectId());
        assertEquals(TimeoutException.class.getName(), fault.getCode());
        assertTrue(fault.getTimestamp() > 0);
    }
    
    @Test
    public void testMapWithProcessingTimeExceeded() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        conf.set(LOG_FAULT_PROCESSING_TIME_THRESHOLD_SECS, String.valueOf(1));
        
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        String id = "id";
        DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
        docContentBuilder.setId(id);
        docContentBuilder.setPdf(ByteBuffer.wrap(getContent(PDF_FILE)));
        
        // execute
        mapper.map(new AvroKey<>(docContentBuilder.build()), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(multipleOutputs, times(2)).write(mosKeyCaptor.capture(), mosValueCaptor.capture());
        // doc meta
        assertEquals(conf.get(NAMED_OUTPUT_META), mosKeyCaptor.getAllValues().get(0));
        ExtractedDocumentMetadata docMeta = (ExtractedDocumentMetadata) mosValueCaptor.getAllValues().get(0).datum();
        assertNotNull(docMeta);
        assertEquals(id, docMeta.getId());
        assertTrue(StringUtils.isNotBlank(docMeta.getText()));
        // fault
        assertEquals(conf.get(NAMED_OUTPUT_FAULT), mosKeyCaptor.getAllValues().get(1));
        Fault fault = (Fault) mosValueCaptor.getAllValues().get(1).datum();
        assertNotNull(fault);
        assertEquals(id, fault.getInputObjectId());
        assertEquals(FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED, fault.getCode());
        assertTrue(fault.getTimestamp() > 0);
        assertNotNull(fault.getSupplementaryData());
        assertEquals(1, fault.getSupplementaryData().size());
        assertNotNull(fault.getSupplementaryData().get(FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME));
    }
    
    @Test
    public void testCleanup() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(NAMED_OUTPUT_META, "meta");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        conf.set(NAMED_OUTPUT_FAULT, "fault");
        
        doReturn(conf).when(context).getConfiguration();
        doReturn(invalidPdfCounter).when(context).getCounter(INVALID_PDF_HEADER);
        mapper.setup(context);
        
        // execute
        mapper.cleanup(context);
        
        // assert
        verify(multipleOutputs, times(1)).close();
    }
    
    // --------------------------------------- PRIVATE ----------------------------------------
    
    private byte[] getContent(String location) throws IOException {
        return IOUtils.toByteArray(ClassPathResourceProvider.getResourceInputStream(location));
    }
    
    
}
