package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

public class OrganizationConverterTest {
    
    
    private OrganizationConverter converter = new OrganizationConverter();
    
    private Organization organization = createOrganization();
    
    
    
    
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
        assertOrganization("Interdisciplinary Centre", affMatchOrg);
    }
    
    
    @Test
    public void convert_null_properties() {
        
        // given
        
        organization.setName(null);
        organization.setShortName(null);
        organization.setCountryCode(null);
        organization.setCountryName(null);
        organization.setWebsiteUrl(null);
        

        // execute 
        
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        // assert
        
        assertEquals(organization.getId(), affMatchOrg.getId());
        assertEquals("", affMatchOrg.getName());
        assertEquals("", affMatchOrg.getShortName());
        assertEquals("", affMatchOrg.getCountryName());
        assertEquals("", affMatchOrg.getCountryCode());
        assertEquals("", affMatchOrg.getWebsiteUrl());
        
        
    }
    
    
    @Test
    public void convert_missing_legal_name() {
        
        // given
        organization.setName(" missing legal namE");
        
        
        // execute
        AffMatchOrganization affMatchOrg = converter.convert(organization);
        
        
        // assert
        assertOrganization("", affMatchOrg);
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

    private void assertOrganization(String expectedOrgName, AffMatchOrganization affMatchOrg) {
        assertEquals(organization.getId(), affMatchOrg.getId());
        assertEquals(expectedOrgName, affMatchOrg.getName());
        assertEquals("ICM", affMatchOrg.getShortName());
        assertEquals("Poland", affMatchOrg.getCountryName());
        assertEquals("PL", affMatchOrg.getCountryCode());
        assertEquals("www.icm.edu.pl", affMatchOrg.getWebsiteUrl());
    }

}
