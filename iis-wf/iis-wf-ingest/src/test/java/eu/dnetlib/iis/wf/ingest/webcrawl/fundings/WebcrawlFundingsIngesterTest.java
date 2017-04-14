package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class WebcrawlFundingsIngesterTest {

    private final static String EXPECTED_OUTPUT_ROOT_LOCATION = "eu/dnetlib/iis/wf/ingest/webcrawl/fundings/data/";
    
    @Mock
    private Context context;
    
    @Captor
    private ArgumentCaptor<AvroKey<DocumentText>> resultCaptor;
    
    
    private final WebcrawlFundingsIngester ingester = new WebcrawlFundingsIngester();

    
    // ------------------------------------- TESTS -----------------------------------
    
    @Test
    public void testMapOnValidWebcrawlResources() throws Exception {
        // given
        List<DocumentText> webcrawlResources = loadResources("wos_document_text.json");
        
        // execute
        for (DocumentText text : webcrawlResources) {
            ingester.map(new AvroKey<DocumentText>(text), null, context);    
        }
        
        // assert
        verify(context, times(3)).write(resultCaptor.capture(), any());
        
        DocumentText value = resultCaptor.getAllValues().get(0).datum();
        assertNotNull(value);
        assertEquals("id-1", value.getId().toString());
        assertEquals("\"This work was partially supported by the 7th EC Framework Programme "
                + "Project \"\"NEWTBVac\"\" Grant #241745.\"", 
                value.getText());
        
        value = resultCaptor.getAllValues().get(1).datum();
        assertNotNull(value);
        assertEquals("id-2", value.getId().toString());
        assertEquals("This work was supported by the Archimedes Center for Modeling, "
                + "Analysis and Computation (ACMAC) (project FP7-REGPOT-2009-1).", 
                value.getText());
        
        value = resultCaptor.getAllValues().get(2).datum();
        assertNotNull(value);
        assertEquals("id-3", value.getId().toString());
        assertEquals("This work was supported by the Archimedes Center for Modeling, "
                + "Analysis and Computation (ACMAC) (project FP7-REGPOT-2009-1).\n" + 
                "\"This work is part of the research program of the \"\"Stichting voor "
                + "Fundamenteel Onderzoek der Materie (FOM)\"\", which is financially "
                + "supported by the \"\"Nederlandse organisatie voor Wetenschappelijk "
                + "Onderzock (NWO)\"\". D.P. thanks the European Commission for support "
                + "with a Marie Curic grant (Project No. TERASPIN 039223).\"", 
                value.getText());
    }

    @Test
    public void testMapOnInvalidWebcrawlResources() throws Exception {
        // given
        List<DocumentText> webcrawlResources = loadResources("wos_document_text_broken.json");
        
        // execute
        for (DocumentText text : webcrawlResources) {
            ingester.map(new AvroKey<DocumentText>(text), null, context);    
        }
        
        // assert
        verify(context, times(0)).write(any(), any());
    }
    
    // ------------------------------------- TESTS -----------------------------------
    
    private List<DocumentText> loadResources(String fileName) {
        return JsonUtils.convertToList(EXPECTED_OUTPUT_ROOT_LOCATION + fileName, 
                DocumentText.SCHEMA$, DocumentText.class);
    }
    
}
