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

public class OrganizationConverterTest {

    
    @InjectMocks
    private OrganizationConverter converter = new OrganizationConverter();
    
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
    
    
    private Organization organization;
    
    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
    
        organization = createOrganization();
        
        when(organizationNameNormalizer.normalize(organization.getName().toString())).thenReturn("interdyscyplinary centre");
        when(organizationShortNameNormalizer.normalize(organization.getShortName().toString())).thenReturn("icm");
        when(countryNameNormalizer.normalize(organization.getCountryName().toString())).thenReturn("poland");
        when(countryCodeNormalizer.normalize(organization.getCountryCode().toString())).thenReturn("pl");
        when(websiteUrlNormalizer.normalize(organization.getWebsiteUrl().toString())).thenReturn("icm.edu.pl");
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_null() {
        
        // execute
        converter.convert(null);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_organization_id() {
        
        // given
        organization.setId(" ");
        
        // execute
        converter.convert(organization);
        
    }
    
    
    @Test
    public void convert() {
        
        // execute
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        
        // assert
        assertOrg("interdyscyplinary centre", affMatchOrg);
    }
    
    
    @Test
    public void convert_null_properties() {
        
        // given
        
        organization.setName(null);
        organization.setShortName(null);
        organization.setCountryCode(null);
        organization.setCountryName(null);
        organization.setWebsiteUrl(null);
        

        when(organizationNameNormalizer.normalize("")).thenReturn("X");
        when(organizationShortNameNormalizer.normalize("")).thenReturn("X");
        when(countryNameNormalizer.normalize("")).thenReturn("X");
        when(countryCodeNormalizer.normalize("")).thenReturn("X");
        when(websiteUrlNormalizer.normalize("")).thenReturn("X");
        
        
        
        // execute 
        
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        // assert
        
        assertEquals(organization.getId(), affMatchOrg.getId());
        assertEquals("X", affMatchOrg.getName());
        assertEquals("X", affMatchOrg.getShortName());
        assertEquals("X", affMatchOrg.getCountryName());
        assertEquals("X", affMatchOrg.getCountryCode());
        assertEquals("X", affMatchOrg.getWebsiteUrl());
        
        
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
        org.setName("Interdisciplinary Centre");
        org.setShortName("ICM");
        org.setCountryCode("PL");
        org.setCountryName("Poland");
        org.setWebsiteUrl("www.icm.edu.pl");
        
        return org;
        
    }

    private void assertOrg(String expectedOrgName, AffMatchOrganization affMatchOrg) {
        assertEquals(organization.getId(), affMatchOrg.getId());
        assertEquals(expectedOrgName, affMatchOrg.getName());
        assertEquals("icm", affMatchOrg.getShortName());
        assertEquals("poland", affMatchOrg.getCountryName());
        assertEquals("pl", affMatchOrg.getCountryCode());
        assertEquals("icm.edu.pl", affMatchOrg.getWebsiteUrl());
    }

}
