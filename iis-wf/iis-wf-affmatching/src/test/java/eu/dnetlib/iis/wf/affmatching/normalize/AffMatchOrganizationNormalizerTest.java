package eu.dnetlib.iis.wf.affmatching.normalize;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchOrganizationNormalizerTest {
    
    @InjectMocks
    private AffMatchOrganizationNormalizer normalizer = new AffMatchOrganizationNormalizer();
    
    @Mock
    private StringNormalizer organizationNameNormalizer;

    @Mock
    private StringNormalizer organizationShortNameNormalizer;
    
    @Mock
    private StringNormalizer countryNameNormalizer;
    
    @Mock
    private StringNormalizer countryCodeNormalizer;
    
    @Mock
    private StringNormalizer websiteUrlNormalizer;
    
    
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
        
        AffMatchOrganization org = new AffMatchOrganization("XXX");
        org.setName("ICM_LONG");
        org.setShortName("ICM");
        org.setCountryCode("PL");
        org.setCountryName("Poland");
        org.setWebsiteUrl("www.icm.edu.pl");
        
        when(organizationNameNormalizer.normalize("ICM_LONG")).thenReturn("icm long");
        when(organizationShortNameNormalizer.normalize("ICM")).thenReturn("icm");
        when(countryCodeNormalizer.normalize("PL")).thenReturn("pl");
        when(countryNameNormalizer.normalize("Poland")).thenReturn("poland");
        when(websiteUrlNormalizer.normalize("www.icm.edu.pl")).thenReturn("icm.edu.pl");
        
        
        // execute
        
        AffMatchOrganization normalizedOrg = normalizer.normalize(org);
        
        
        // assert
        
        assertEquals("icm long", normalizedOrg.getName());
        assertEquals("icm", normalizedOrg.getShortName());
        assertEquals("pl", normalizedOrg.getCountryCode());
        assertEquals("poland", normalizedOrg.getCountryName());
        assertEquals("icm.edu.pl", normalizedOrg.getWebsiteUrl());
        
    }

}
