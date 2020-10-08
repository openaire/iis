package eu.dnetlib.iis.wf.affmatching.normalize;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class AffMatchAffiliationNormalizerTest {
    
    @InjectMocks
    private AffMatchAffiliationNormalizer normalizer = new AffMatchAffiliationNormalizer();
    
    @Mock
    private StringNormalizer organizationNameNormalizer;
    
    @Mock
    private StringNormalizer countryNameNormalizer;
    
    @Mock
    private StringNormalizer countryCodeNormalizer;

    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> normalizer.normalize(null));
        
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

        assertNotSame(aff, normalizedAff);
        assertEquals("XXX", normalizedAff.getDocumentId());
        assertEquals(1, normalizedAff.getPosition());
        assertEquals("icm long", normalizedAff.getOrganizationName());
        assertEquals("pl", normalizedAff.getCountryCode());
        assertEquals("poland", normalizedAff.getCountryName());
        
    }

}
