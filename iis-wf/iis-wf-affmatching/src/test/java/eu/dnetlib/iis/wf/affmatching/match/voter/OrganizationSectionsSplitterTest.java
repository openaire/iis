package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.iterableWithSize;

import org.junit.Test;

/**
 * @author madryk
 */
public class OrganizationSectionsSplitterTest {

    
    //------------------------ TESTS --------------------------
    
    @Test
    public void splitToSections() {
        
        // execute & assert
        
        assertThat(OrganizationSectionsSplitter.splitToSections(""), iterableWithSize(0));
        assertThat(OrganizationSectionsSplitter.splitToSections(","), iterableWithSize(0));
        assertThat(OrganizationSectionsSplitter.splitToSections("aa"), contains("aa"));
        assertThat(OrganizationSectionsSplitter.splitToSections("aa, bb, cc"), contains("aa", "bb", "cc"));
        assertThat(OrganizationSectionsSplitter.splitToSections("aa; bb; cc"), contains("aa", "bb", "cc"));
        assertThat(OrganizationSectionsSplitter.splitToSections("aa; bb; ltd"), contains("aa", "bb"));
        assertThat(OrganizationSectionsSplitter.splitToSections("aa; inc; cc"), contains("aa", "cc"));
        
    }
}
