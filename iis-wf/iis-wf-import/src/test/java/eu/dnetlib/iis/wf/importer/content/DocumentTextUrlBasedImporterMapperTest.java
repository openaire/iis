package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.importer.content.DocumentTextUrlBasedImporterMapper.InvalidRecordCounters;


/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class DocumentTextUrlBasedImporterMapperTest {

    private DocumentTextUrlBasedImporterMapper mapper;
    
    private String content;
    
    @Mock
    private Context context;
    
    @Mock
    private Counter sizeExceededCounter;
    
    @Mock
    private Counter sizeInvalidCounter;
    
    @Mock
    private Counter unavailableCounter;
    
    @Captor
    private ArgumentCaptor<AvroKey<DocumentText>> keyCaptor;
    
    @Captor
    private ArgumentCaptor<NullWritable> valueCaptor;
    
    
    @Before
    public void init() throws Exception {
        
        content = "test content";
        
        mapper = new DocumentTextUrlBasedImporterMapper() {
            
            @Override
            protected byte[] getContent(String url) throws IOException, InvalidSizeException {
                return content.getBytes("utf8");
            }
            
        };
        
        doReturn(sizeExceededCounter).when(context).getCounter(InvalidRecordCounters.SIZE_EXCEEDED);
        doReturn(sizeInvalidCounter).when(context).getCounter(InvalidRecordCounters.SIZE_INVALID);
        doReturn(unavailableCounter).when(context).getCounter(InvalidRecordCounters.UNAVAILABLE);
    }
    
    // --------------------------------- TESTS ---------------------------------
    
    @Test
    public void testObtainContent() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        String id = "contentId";
        String url = "contentUrl";
        DocumentContentUrl docContentUrl = new DocumentContentUrl();
        docContentUrl.setId(id);
        docContentUrl.setUrl(url);
        docContentUrl.setContentSizeKB(1l);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<DocumentContentUrl>(docContentUrl), null, context);
        
        // assert
        verify(context, times(1)).write(keyCaptor.capture(), valueCaptor.capture());
        assertTrue(NullWritable.get() == valueCaptor.getValue());
        DocumentText docContent = keyCaptor.getValue().datum();
        assertEquals(id, docContent.getId());
        assertEquals(content, docContent.getText());
        verify(sizeExceededCounter, never()).increment(1);
        verify(sizeInvalidCounter, never()).increment(1);
        verify(unavailableCounter, never()).increment(1);
    }
    
    @Test
    public void testContentSizeInvalid() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        String id = "contentId";
        String url = "contentUrl";
        DocumentContentUrl docContentUrl = new DocumentContentUrl();
        docContentUrl.setId(id);
        docContentUrl.setUrl(url);
        docContentUrl.setContentSizeKB(-1l);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<DocumentContentUrl>(docContentUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(sizeExceededCounter, never()).increment(1);
        verify(sizeInvalidCounter, times(1)).increment(1);
        verify(unavailableCounter, never()).increment(1);
    }
    
    @Test
    public void testContentSizeInvalidThrown() throws Exception {
        // given
        mapper = new DocumentTextUrlBasedImporterMapper() {
            
            @Override
            protected byte[] getContent(String url) throws IOException, InvalidSizeException {
                throw new InvalidSizeException();
            }
            
        };
        
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        String id = "contentId";
        String url = "contentUrl";
        DocumentContentUrl docContentUrl = new DocumentContentUrl();
        docContentUrl.setId(id);
        docContentUrl.setUrl(url);
        docContentUrl.setContentSizeKB(1l);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<DocumentContentUrl>(docContentUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(sizeExceededCounter, never()).increment(1);
        verify(sizeInvalidCounter, times(1)).increment(1);
        verify(unavailableCounter, never()).increment(1);
    }
    
    @Test
    public void testIOExceptionThrown() throws Exception {
        // given
        mapper = new DocumentTextUrlBasedImporterMapper() {
            
            @Override
            protected byte[] getContent(String url) throws IOException, InvalidSizeException {
                throw new IOException();
            }
            
        };
        
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        String id = "contentId";
        String url = "contentUrl";
        DocumentContentUrl docContentUrl = new DocumentContentUrl();
        docContentUrl.setId(id);
        docContentUrl.setUrl(url);
        docContentUrl.setContentSizeKB(1l);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<DocumentContentUrl>(docContentUrl), null, context);

        // assert
        verify(context, never()).write(any(), any());
        verify(sizeExceededCounter, never()).increment(1);
        verify(sizeInvalidCounter, never()).increment(1);
        verify(unavailableCounter, times(1)).increment(1);
    }
    
    @Test
    public void testContentSizeExceeded() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(IMPORT_CONTENT_MAX_FILE_SIZE_MB, "1");
        doReturn(conf).when(context).getConfiguration();
        
        String id = "contentId";
        String url = "contentUrl";
        DocumentContentUrl docContentUrl = new DocumentContentUrl();
        docContentUrl.setId(id);
        docContentUrl.setUrl(url);
        docContentUrl.setContentSizeKB(1025l);
        mapper.setup(context);
        
        // execute
        mapper.map(new AvroKey<DocumentContentUrl>(docContentUrl), null, context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(sizeExceededCounter, times(1)).increment(1);
        verify(sizeInvalidCounter, never()).increment(1);
        verify(unavailableCounter, never()).increment(1);
        
    }

}
