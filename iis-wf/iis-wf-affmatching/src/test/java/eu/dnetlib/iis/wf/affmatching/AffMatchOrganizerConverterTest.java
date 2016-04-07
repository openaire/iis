package eu.dnetlib.iis.wf.affmatching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchOrganizerConverterTest {

    
    @InjectMocks
    private AffMatchOrganizationConverter converter = new AffMatchOrganizationConverter();
    
    @Mock
    private StringNormalizer organizationNameNormalizer;

    @Mock
    private StringNormalizer countryNameNormalizer;
    
    @Mock
    private StringNormalizer websiteUrlNormalizer;
    
    
    private Organization organization;
    
    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
    
        organization = createOrganization();
        
        when(organizationNameNormalizer.normalize(organization.getName().toString())).thenReturn("interdyscyplinary centre");
        when(countryNameNormalizer.normalize(organization.getCountryName().toString())).thenReturn("poland");
        when(websiteUrlNormalizer.normalize(organization.getWebsiteUrl().toString())).thenReturn("icm.edu.pl");
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_null() {
        
        // execute
        converter.convert(null);
        
    }
    
    
    @Test
    public void convert() {
        
        // execute
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        
        // assert
        assertOrg("interdyscyplinary centre", affMatchOrg);
    }
    
    
    
    @Test
    public void convert_missing_legal_name() {
        
        // given
        when(organizationNameNormalizer.normalize(organization.getName().toString())).thenReturn("missing legal name");
        
        
        // execute
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        
        // assert
        assertOrg("", affMatchOrg);
    }




    
    
    //------------------------ PRIVATE --------------------------
    
    private Organization createOrganization() {
        
        Organization org = new Organization();
        
        org.setId("ABC123");
        org.setName("Interdyscyplinary Centre");
        org.setShortName("ICM");
        org.setCountryCode("PL");
        org.setCountryName("Poland");
        org.setWebsiteUrl("www.icm.edu.pl");
        
        return org;
        
    }

    private void assertOrg(String expectedOrgName, AffMatchOrganization affMatchOrg) {
        assertEquals(organization.getId(), affMatchOrg.getId());
        assertEquals(expectedOrgName, affMatchOrg.getName());
        assertEquals(organization.getShortName(), affMatchOrg.getShortName());
        assertEquals("poland", affMatchOrg.getCountryName());
        assertEquals(organization.getCountryCode(), affMatchOrg.getCountryCode());
        assertEquals("icm.edu.pl", affMatchOrg.getWebsiteUrl());
    }

}
