package eu.dnetlib.iis.wf.affmatching.bucket;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class AffiliationOrgNameBucketHasherTest {

    @InjectMocks
    private AffiliationOrgNameBucketHasher hasher = new AffiliationOrgNameBucketHasher();
    
    @Mock
    private BucketHasher<String> stringHasher;
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> hasher.hash(null));
    }
    
    
    @Test
    public void hash_organizationName_null() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> hasher.hash(aff));
    
    }
    
    
    @Test
    public void hash_organizationName_blank() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        aff.setOrganizationName(" ");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> hasher.hash(aff));
    }
    
    
    @Test
    public void hash() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        aff.setOrganizationName("ICM");
        
        when(stringHasher.hash("ICM")).thenReturn("HASH");
        
        
        // execute
        
        String hash = hasher.hash(aff);
        
        
        // assert
        
        assertEquals("HASH", hash);
        
    }
    
    
    
}
