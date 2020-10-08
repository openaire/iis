package eu.dnetlib.iis.common.report.test;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.schemas.ReportEntry;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Service that checks whether a given list of {@link ReportEntry} objects complies with
 * some expected values (or expressions).  
 * 
 * @author Łukasz Dumiszewski
*/

public class ReportEntryMatcher {

    private ValueSpecMatcher valueSpecMatcher = new ValueSpecMatcher();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Checks if the given actualEntries are the same as the expected ones passed as expectedEntrySpecs.
     * The given actual {@link ReportEntry} is considered to be equal to the given expected one if their keys and types are the same
     * and if the values match. It is decided by {@link ValueSpecMatcher#matches(String, String)} whether the values match or not.
     *  
     */
    public void checkMatch(List<ReportEntry> actualEntries, List<ReportEntry> expectedEntrySpecs) {
    
        Preconditions.checkNotNull(actualEntries);
        Preconditions.checkNotNull(expectedEntrySpecs);

        assertEquals(expectedEntrySpecs.size(), actualEntries.size(),
                String.format("The number of the expected report entries (%s) is not equal to the number " +
                        "of the actual entries (%s).\n\n%s", expectedEntrySpecs.size(), actualEntries.size(), "expected:" + expectedEntrySpecs + "actual: " + actualEntries));
        
        for (ReportEntry expectedEntry : expectedEntrySpecs) {
            
            if (!containsEntry(actualEntries, expectedEntry)) {
                fail(String.format("The actual report entries do NOT contain the expected ones\n\nActual:%s\n\nExpected:%s",
                                    actualEntries, expectedEntrySpecs)); 
            }
            
        }
        
    }

    
    //------------------------ PRIVATE --------------------------
    
    private boolean containsEntry(List<ReportEntry> actualEntries, ReportEntry expectedEntry) {
        
        for (ReportEntry actualEntry : actualEntries) {
            
            if (checkEqual(actualEntry, expectedEntry)) {
                return true;
            }
            
        }
        
        return false;
        
    }


    private boolean checkEqual(ReportEntry actualEntry, ReportEntry expectedEntry) {
        return actualEntry.getKey().equals(expectedEntry.getKey()) 
                    && actualEntry.getType().equals(expectedEntry.getType())
                    && valueSpecMatcher.matches(actualEntry.getValue().toString(), expectedEntry.getValue().toString());
    }
    
    
    
}
