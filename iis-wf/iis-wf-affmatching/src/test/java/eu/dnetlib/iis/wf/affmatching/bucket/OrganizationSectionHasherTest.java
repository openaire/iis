package eu.dnetlib.iis.wf.affmatching.bucket;

import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class OrganizationSectionHasherTest {


    private OrganizationSectionHasher orgSectionHasher = new OrganizationSectionHasher();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash_university_section() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[] {"medical", "university", "toronto"}, 1);
        
        // execute & assert
        assertEquals("UNImedtor", orgSectionHasher.hash(section));
        
    }
    
    @Test
    public void hash_university_short_section() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[] {"md", "university", "in"}, 1);
        
        // execute & assert
        assertEquals("UNIin_md_", orgSectionHasher.hash(section));
        
    }
    
    @Test
    public void hash_university_section_univ_on_first_position() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[] {"university", "barcelona"}, 0);
        
        // execute & assert
        assertEquals("UNI___bar", orgSectionHasher.hash(section));
        
    }
    
    @Test
    public void hash_university_section_univ_on_last_position() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[] {"washington", "university"}, 1);
        
        // execute & assert
        assertEquals("UNI___was", orgSectionHasher.hash(section));
        
    }

    @Test
    public void hash_unknown_section() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNKNOWN, new String[] {"medical", "college", "school"}, -1);
        
        // execute & assert
        assertEquals("UNKcolmed", orgSectionHasher.hash(section));
        
    }
    
    @Test
    public void hash_unknown_section_short_words() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNKNOWN, new String[] {"a", "bb"}, -1);
        
        // execute & assert
        assertEquals("UNKa__bb_", orgSectionHasher.hash(section));
        
    }
    
    @Test
    public void hash_unknown_section_one_word() {
        
        // given
        OrganizationSection section = new OrganizationSection(OrgSectionType.UNKNOWN, new String[] {"institution"}, -1);
        
        // execute & assert
        assertEquals("UNK___ins", orgSectionHasher.hash(section));
        
    }
    
    
}
