package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class OrganizationNameBucketHasherTest {

    @InjectMocks
    private OrganizationNameBucketHasher hasher = new OrganizationNameBucketHasher();
    
    @Mock
    private BucketHasher<String> stringHasher;
    
    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void hash_null() {
        
        // execute
        
        hasher.hash(null);
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void hash_name_null() {
        
        // given
        
        AffMatchOrganization org = new AffMatchOrganization("XXX");
        
        
        // execute
    
        hasher.hash(org);
    
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void hash_name_blank() {
        
        // given
        
        AffMatchOrganization org = new AffMatchOrganization("XXX");
        org.setName(" ");
        
        // execute
        
        hasher.hash(org);
    }
    
    
    @Test
    public void hash() {
        
        // given
        
        AffMatchOrganization org = new AffMatchOrganization("XXX");
        org.setName("ICM");
        
        when(stringHasher.hash("ICM")).thenReturn("HASH");
        
        
        // execute
        
        String hash = hasher.hash(org);
        
        
        // assert
        
        assertEquals("HASH", hash);
        
    }
    
    
    
}
