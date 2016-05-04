package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffiliationNameFirstLettersBucketHasherTest {

    @InjectMocks
    private AffiliationOrgNameFirstLettersBucketHasher hasher = new AffiliationOrgNameFirstLettersBucketHasher();
    
    @Mock
    private StringPartFirstLettersHasher stringPartFirstLettersHasher;
    
    
    
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
    public void hash_organizationName_null() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        
        
        // execute
    
        hasher.hash(aff);
    
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void hash_organizationName_blank() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        aff.setOrganizationName(" ");
        
        // execute
        
        hasher.hash(aff);
    }
    
    
    @Test
    public void hash() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        aff.setOrganizationName("ICM");
        
        when(stringPartFirstLettersHasher.hash("ICM")).thenReturn("HASH");
        
        
        // execute
        
        String hash = hasher.hash(aff);
        
        
        // assert
        
        assertEquals("HASH", hash);
        
    }
    
    
    
}
