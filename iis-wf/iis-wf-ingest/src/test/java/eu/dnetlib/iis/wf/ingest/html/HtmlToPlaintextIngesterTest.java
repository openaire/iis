package eu.dnetlib.iis.wf.ingest.html;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class HtmlToPlaintextIngesterTest {

    @Mock
    private Context context;

    @Captor
    private ArgumentCaptor<AvroKey<DocumentText>> resultCaptor;

    private final HtmlToPlaintextIngester ingester = new HtmlToPlaintextIngester();

    private final String id = "id";

    // ------------------------------------- TESTS -----------------------------------

    @Test
    public void testMapOnPlainText() throws Exception {
        // given
        String text = "plain text";
        DocumentText key = DocumentText.newBuilder().setId(id).setText(text).build();

        // execute
        ingester.map(new AvroKey<DocumentText>(key), null, context);

        // assert
        verify(context, times(1)).write(resultCaptor.capture(), any());
        DocumentText value = resultCaptor.getValue().datum();
        assertNotNull(   value);
        assertEquals(id, value.getId());
        assertEquals(text, value.getText());
    }
    
    @Test
    public void testMapOnHtml() throws Exception {
        // given
        String text = "<br/><b>sample text</b><br/>";
        DocumentText key = DocumentText.newBuilder().setId(id).setText(text).build();
        
        // execute
        ingester.map(new AvroKey<DocumentText>(key), null, context);
        
        // assert
        verify(context, times(1)).write(resultCaptor.capture(), any());
        DocumentText value = resultCaptor.getValue().datum();
        assertNotNull(value);
        assertEquals(id, value.getId());
        assertEquals("sample text", value.getText());
    }

}
