package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class AffiliationMainSectionBucketHasherTest {

    @InjectMocks
    private AffiliationMainSectionBucketHasher affMainSectionBucketHasher = new AffiliationMainSectionBucketHasher();
    
    @Mock
    private OrganizationSectionsSplitter sectionsSplitter;
    
    @Mock
    private OrganizationSectionHasher sectionHasher;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC_ID", 1);
    
    private String organizationName = "ORG_NAME";
    
    
    @Before
    public void setup() {
        affiliation.setOrganizationName(organizationName);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash() {
        
        // given
        
        OrganizationSection section1 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown1"}, 0);
        OrganizationSection section2 = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[]{"university2"}, 0);
        OrganizationSection section3 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown3"}, 0);
        OrganizationSection section4 = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[]{"university4"}, 0);
        
        String section2_hash = "section2_hash";
        
        when(sectionsSplitter.splitToSectionsDetailed(organizationName)).thenReturn(ImmutableList.of(section1, section2, section3, section4));
        when(sectionHasher.hash(section2)).thenReturn(section2_hash);
        
        
        // execute
        
        String retHash = affMainSectionBucketHasher.hash(affiliation);
        
        
        // assert
        
        assertEquals(section2_hash, retHash);
        
        verify(sectionsSplitter).splitToSectionsDetailed(organizationName);
        verify(sectionHasher).hash(section2);
    }
    
    @Test
    public void hash_fallback_section() {
        
        // given
        
        OrganizationSection section1 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown1"}, 0);
        OrganizationSection section2 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown2"}, 0);
        OrganizationSection section3 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown3"}, 0);
        
        String section3_hash = "section3_hash";
        
        when(sectionsSplitter.splitToSectionsDetailed(organizationName)).thenReturn(ImmutableList.of(section1, section2, section3));
        when(sectionHasher.hash(section3)).thenReturn(section3_hash);
        
        
        // execute
        
        String retHash = affMainSectionBucketHasher.hash(affiliation);
        
        
        // assert
        
        assertEquals(section3_hash, retHash);
        
        verify(sectionsSplitter).splitToSectionsDetailed(organizationName);
        verify(sectionHasher).hash(section3);
    }
    
}
