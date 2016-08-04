package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;

import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class ReportValueJsonConverterManagerTest {

    private ReportValueJsonConverterManager converterManager = new ReportValueJsonConverterManager();
    
    @Mock
    private ReportValueJsonConverter converter1;
    
    @Mock
    private ReportValueJsonConverter converter2;
    
    
    private ReportEntry reportEntry = ReportEntryFactory.createCounterReportEntry("report.key", 34);
    
    
    @Before
    public void setup() {
        converterManager.setConverters(ImmutableList.of(converter1, converter2));
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = IllegalArgumentException.class)
    public void convertValue_NO_APPLICABLE_CONVERTER() {
        
        // given
        
        when(converter1.isApplicable(reportEntry.getType())).thenReturn(false);
        when(converter2.isApplicable(reportEntry.getType())).thenReturn(false);
        
        // execute
        
        converterManager.convertValue(reportEntry);
    }
    
    @Test
    public void convertValue_USE_FIRST_CONVERTER() {
        
        // given
        
        when(converter1.isApplicable(reportEntry.getType())).thenReturn(true);
        when(converter2.isApplicable(reportEntry.getType())).thenReturn(false);
        
        JsonElement json = mock(JsonElement.class);
        when(converter1.convertValue(reportEntry)).thenReturn(json);
        
        
        // execute
        
        JsonElement retJson = converterManager.convertValue(reportEntry);
        
        
        // assert
        
        assertTrue(retJson == json);
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
        
        assertTrue(retJson == json);
    }
}
