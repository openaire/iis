package eu.dnetlib.iis.wf.affmatching.bucket;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class OrganizationNameBucketHasherTest {

    @InjectMocks
    private OrganizationNameBucketHasher hasher = new OrganizationNameBucketHasher();
    
    @Mock
    private BucketHasher<String> stringHasher;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction; 
    
    @Mock
    private AffMatchOrganization organization;
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash_org_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> hasher.hash(null));
    }
    
    
    @Test
    public void hash_org_names_empty() {
        
        // given
        
        when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList());
        
        
        // execute
    
        assertNull(hasher.hash(organization));
    
    }
    
    
    @Test
    public void hash() {
        
        // given
        
        when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList("ICM", "PWR"));
        when(stringHasher.hash("ICM")).thenReturn("HASH");
        
        
        // execute
        
        String hash = hasher.hash(organization);
        
        
        // assert
        
        assertEquals("HASH", hash);
        
    }
    
    
    
}
