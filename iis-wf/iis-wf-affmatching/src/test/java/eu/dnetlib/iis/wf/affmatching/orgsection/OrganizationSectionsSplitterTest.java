package eu.dnetlib.iis.wf.affmatching.orgsection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * @author madryk
 */
public class OrganizationSectionsSplitterTest {

    private OrganizationSectionsSplitter sectionsSplitter = new OrganizationSectionsSplitter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void splitToSections() {
        
        // execute & assert
        
        assertThat(sectionsSplitter.splitToSections(""), iterableWithSize(0));
        assertThat(sectionsSplitter.splitToSections(","), iterableWithSize(0));
        assertThat(sectionsSplitter.splitToSections("aa"), contains("aa"));
        assertThat(sectionsSplitter.splitToSections("aa, bb, cc"), contains("aa", "bb", "cc"));
        assertThat(sectionsSplitter.splitToSections("aa; bb; cc"), contains("aa", "bb", "cc"));
        assertThat(sectionsSplitter.splitToSections("aa; bb; ltd"), contains("aa", "bb"));
        assertThat(sectionsSplitter.splitToSections("aa; inc; cc"), contains("aa", "cc"));
        
    }
    
    @Test
    public void splitToSectionsDetailed() {
        
        // execute
        
        List<OrganizationSection> sections = sectionsSplitter.splitToSectionsDetailed("university of california, department of chemistry");
        
        
        // assert
        
        assertEquals(2, sections.size());
        
        OrganizationSection expectedSection1 = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[]{"university", "of", "california"}, 0);
        assertEquals(expectedSection1, sections.get(0));
        
        OrganizationSection expectedSection2 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"department", "of", "chemistry"}, -1);
        assertEquals(expectedSection2, sections.get(1));
    }
    
    @Test
    public void splitToSectionsDetailed_university_section() {
        
        // execute & assert
        
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("university of california").get(0).getType());
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("universita di roma").get(0).getType());
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("universidade do porto").get(0).getType());
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("universite libre de bruxelles").get(0).getType());
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("uniwersytet warszawski").get(0).getType());
        assertEquals(OrgSectionType.UNIVERSITY, sectionsSplitter.splitToSectionsDetailed("technische universiteit eindhoven").get(0).getType());
        
    }
}
