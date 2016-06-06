package eu.dnetlib.iis.wf.affmatching.normalize;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchAffiliationNormalizerTest {
    
    @InjectMocks
    private AffMatchAffiliationNormalizer normalizer = new AffMatchAffiliationNormalizer();
    
    @Mock
    private StringNormalizer organizationNameNormalizer;
    
    @Mock
    private StringNormalizer countryNameNormalizer;
    
    @Mock
    private StringNormalizer countryCodeNormalizer;

    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void normalize_null() {
        
        // execute
        
        normalizer.normalize(null);
        
    }
    
    
    @Test
    public void normalize() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("XXX", 1);
        aff.setOrganizationName("ICM_LONG");
        aff.setCountryCode("PL");
        aff.setCountryName("Poland");
        
        when(organizationNameNormalizer.normalize("ICM_LONG")).thenReturn("icm long");
        when(countryCodeNormalizer.normalize("PL")).thenReturn("pl");
        when(countryNameNormalizer.normalize("Poland")).thenReturn("poland");
        
        
        // execute
        
        AffMatchAffiliation normalizedAff = normalizer.normalize(aff);
        
        
        // assert
        
        assertTrue(aff != normalizedAff);
        assertEquals("XXX", normalizedAff.getDocumentId());
        assertEquals(1, normalizedAff.getPosition());
        assertEquals("icm long", normalizedAff.getOrganizationName());
        assertEquals("pl", normalizedAff.getCountryCode());
        assertEquals("poland", normalizedAff.getCountryName());
        
    }

}
