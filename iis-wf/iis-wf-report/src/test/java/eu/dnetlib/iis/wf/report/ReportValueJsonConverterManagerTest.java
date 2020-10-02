package eu.dnetlib.iis.wf.report;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class ReportValueJsonConverterManagerTest {

    private ReportValueJsonConverterManager converterManager = new ReportValueJsonConverterManager();
    
    @Mock
    private ReportValueJsonConverter converter1;
    
    @Mock
    private ReportValueJsonConverter converter2;
    
    
    private ReportEntry reportEntry = ReportEntryFactory.createCounterReportEntry("report.key", 34);
    
    
    @BeforeEach
    public void setup() {
        converterManager.setConverters(ImmutableList.of(converter1, converter2));
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void convertValue_NO_APPLICABLE_CONVERTER() {
        
        // given
        
        when(converter1.isApplicable(reportEntry.getType())).thenReturn(false);
        when(converter2.isApplicable(reportEntry.getType())).thenReturn(false);
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> converterManager.convertValue(reportEntry));
    }
    
    @Test
    public void convertValue_USE_FIRST_CONVERTER() {
        
        // given
        
        when(converter1.isApplicable(reportEntry.getType())).thenReturn(true);
        
        JsonElement json = mock(JsonElement.class);
        when(converter1.convertValue(reportEntry)).thenReturn(json);
        
        
        // execute
        
        JsonElement retJson = converterManager.convertValue(reportEntry);
        
        
        // assert

        assertSame(retJson, json);
    }
    
    @Test
    public void convertValue_USE_SECOND_CONVERTER() {
        
        // given
        
        when(converter1.isApplicable(reportEntry.getType())).thenReturn(false);
        when(converter2.isApplicable(reportEntry.getType())).thenReturn(true);
        
        JsonElement json = mock(JsonElement.class);
        when(converter2.convertValue(reportEntry)).thenReturn(json);
        
        
        // execute
        
        JsonElement retJson = converterManager.convertValue(reportEntry);
        
        
        // assert

        assertSame(retJson, json);
    }
}
