package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class OrganizationNameBucketHasherTest {

    @InjectMocks
    private OrganizationNameBucketHasher hasher = new OrganizationNameBucketHasher();
    
    @Mock
    private BucketHasher<String> stringHasher;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction; 
    
    @Mock
    private AffMatchOrganization organization;
    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void hash_org_null() {
        
        // execute
        
        hasher.hash(null);
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
