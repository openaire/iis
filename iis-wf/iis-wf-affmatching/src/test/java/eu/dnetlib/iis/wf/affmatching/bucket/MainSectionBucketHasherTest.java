package eu.dnetlib.iis.wf.affmatching.bucket;

import com.google.common.collect.ImmutableList;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class MainSectionBucketHasherTest {

    @InjectMocks
    private MainSectionBucketHasher mainSectionBucketHasher = new MainSectionBucketHasher();
    
    @Mock
    private OrganizationSectionsSplitter sectionsSplitter;
    
    @Mock
    private OrganizationSectionHasher sectionHasher;
    
    
    private String organizationName = "ORG_NAME";
    
    
    @BeforeEach
    public void setup() {
        mainSectionBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.FIRST_SECTION);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash() {
        
        // given
        
        OrganizationSection section1 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown1"}, -1);
        OrganizationSection section2 = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[]{"university2"}, 0);
        OrganizationSection section3 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown3"}, -1);
        OrganizationSection section4 = new OrganizationSection(OrgSectionType.UNIVERSITY, new String[]{"university4"}, 0);
        
        String section2_hash = "section2_hash";
        
        when(sectionsSplitter.splitToSectionsDetailed(organizationName)).thenReturn(ImmutableList.of(section1, section2, section3, section4));
        when(sectionHasher.hash(section2)).thenReturn(section2_hash);
        
        
        // execute
        
        String retHash = mainSectionBucketHasher.hash(organizationName);
        
        
        // assert
        
        assertEquals(section2_hash, retHash);
        
        verify(sectionsSplitter).splitToSectionsDetailed(organizationName);
        verify(sectionHasher).hash(section2);
    }
    
    @Test
    public void hash_fallback_first_section() {
        
        // given
        
        OrganizationSection section1 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown1"}, -1);
        OrganizationSection section2 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown2"}, -1);
        OrganizationSection section3 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown3"}, -1);
        
        String section1_hash = "section1_hash";
        
        when(sectionsSplitter.splitToSectionsDetailed(organizationName)).thenReturn(ImmutableList.of(section1, section2, section3));
        when(sectionHasher.hash(section1)).thenReturn(section1_hash);
        
        
        // execute
        
        String retHash = mainSectionBucketHasher.hash(organizationName);
        
        
        // assert
        
        assertEquals(section1_hash, retHash);
        
        verify(sectionsSplitter).splitToSectionsDetailed(organizationName);
        verify(sectionHasher).hash(section1);
    }
    
    @Test
    public void hash_fallback_last_section() {
        
        // given
        
        mainSectionBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.LAST_SECTION);
        
        OrganizationSection section1 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown1"}, -1);
        OrganizationSection section2 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown2"}, -1);
        OrganizationSection section3 = new OrganizationSection(OrgSectionType.UNKNOWN, new String[]{"unknown3"}, -1);
        
        String section3_hash = "section3_hash";
        
        when(sectionsSplitter.splitToSectionsDetailed(organizationName)).thenReturn(ImmutableList.of(section1, section2, section3));
        when(sectionHasher.hash(section3)).thenReturn(section3_hash);
        
        
        // execute
        
        String retHash = mainSectionBucketHasher.hash(organizationName);
        
        
        // assert
        
        assertEquals(section3_hash, retHash);
        
        verify(sectionsSplitter).splitToSectionsDetailed(organizationName);
        verify(sectionHasher).hash(section3);
    }
}
