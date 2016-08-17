package eu.dnetlib.iis.common.report.test;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.common.report.ReportEntryFactory.createDurationReportEntry;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class ReportEntryMatcherTest {

    @InjectMocks
    private ReportEntryMatcher matcher = new ReportEntryMatcher();
    
    @Mock
    private ValueSpecMatcher valueSpecMatcher = new ValueSpecMatcher();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void checkMatch_actualEntries_NULL() {
        
        // given
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createCounterReportEntry("KEY_A", 3));
        
        // execute
        matcher.checkMatch(null, expectedEntrySpecs);
    }
    
    @Test(expected = NullPointerException.class)
    public void checkMatch_expectedEntrySpecs_NULL() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3));
        
        // execute
        matcher.checkMatch(actualEntries, null);
    }
    
    @Test(expected = AssertionError.class)
    public void checkMatch_different_sizes() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_X", 3));
        
        // execute
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
    }
    
    @Test(expected = AssertionError.class)
    public void checkMatch_DONT_MATCH_DIFF_TYPES() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createCounterReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        
        // execute
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
    }
    
    @Test(expected = AssertionError.class)
    public void checkMatch_DONT_MATCH_DIFF_KEYS() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_AAA", 3), createCounterReportEntry("KEY_B", 4));
        
        // execute
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
    }
    
    @Test(expected = AssertionError.class)
    public void checkMatch_DONT_MATCH_DIFF_VALUES() {
        
        // given
        List<ReportEntry> actualEntries = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 4));
        List<ReportEntry> expectedEntrySpecs = Lists.newArrayList(createDurationReportEntry("KEY_A", 3), createCounterReportEntry("KEY_B", 41));
        when(valueSpecMatcher.matches("3", "3")).thenReturn(true);
        when(valueSpecMatcher.matches("4", "41")).thenReturn(false);
        
        // execute
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
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
