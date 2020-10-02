package eu.dnetlib.iis.common.report.test;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.common.report.ReportEntryFactory.createDurationReportEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class ReportEntryMatcherTest {

    @InjectMocks
    private ReportEntryMatcher matcher = new ReportEntryMatcher();
    
    @Mock
    private ValueSpecMatcher valueSpecMatcher = new ValueSpecMatcher();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void checkMatch_actualEntries_NULL() {
        
        // given
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createCounterReportEntry("KEY_A", 3));
        
        // execute
        assertThrows(NullPointerException.class, () -> matcher.checkMatch(null, expectedEntrySpecs));
    }
    
    @Test
    public void checkMatch_expectedEntrySpecs_NULL() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3));
        
        // execute
        assertThrows(NullPointerException.class, () -> matcher.checkMatch(actualEntries, null));
    }
    
    @Test
    public void checkMatch_different_sizes() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_X", 3));
        
        // execute
        assertThrows(AssertionError.class, () -> matcher.checkMatch(actualEntries, expectedEntrySpecs));
    }
    
    @Test
    public void checkMatch_DONT_MATCH_DIFF_TYPES() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        
        // execute
        assertThrows(AssertionError.class, () -> matcher.checkMatch(actualEntries, expectedEntrySpecs));
    }
    
    @Test
    public void checkMatch_DONT_MATCH_DIFF_KEYS() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_AAA", 3), createCounterReportEntry("KEY_B", 4));
        
        // execute
        assertThrows(AssertionError.class, () -> matcher.checkMatch(actualEntries, expectedEntrySpecs));
    }
    
    @Test
    public void checkMatch_DONT_MATCH_DIFF_VALUES() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 41));
        when(valueSpecMatcher.matches("3", "3")).thenReturn(true);
        when(valueSpecMatcher.matches("4", "41")).thenReturn(false);
        
        // execute
        assertThrows(AssertionError.class, () -> matcher.checkMatch(actualEntries, expectedEntrySpecs));
    }
    
    @Test
    public void checkMatch_MATCH() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        when(valueSpecMatcher.matches("3", "3")).thenReturn(true);
        when(valueSpecMatcher.matches("4", "4")).thenReturn(true);
        
        // execute
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
    }
    
}
