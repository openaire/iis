package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.iis.importer.schemas.Organization;

/**
* @author Łukasz Dumiszewski
*/

public class OrganizationConverterTest {

    private OrganizationConverter converter = new OrganizationConverter();
    
    private Logger log = mock(Logger.class);
    
    
    @Before
    public void before() {
        
        Whitebox.setInternalState(OrganizationConverter.class, "log", log);
        
    }
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void buildObject_resolvedOadObject_NULL() throws Exception {
        
        // execute
        
        converter.convert(null);
        
    }
    
    @Test
    public void buildObject_organization_metadata_name_empty() throws Exception {
        
        //given
        
        eu.dnetlib.dhp.schema.oaf.Organization organization = createEmptyOafObject();
        
        // execute & assert
        
        assertNull(converter.convert(organization));
        verify(log).error("skipping: empty organization name");
        
    }

    @Test
    public void buildObject() throws Exception {
        
        //given
        Qualifier country = new Qualifier();
        country.setClassid("PL");
        country.setClassname("Poland");

        eu.dnetlib.dhp.schema.oaf.Organization organization = createOafObject(country,
                createStringField("Interdyscyplinary Centre"), createStringField("ICM"),
                createStringField("www.icm.edu.pl"));
        
        // execute 
        
        Organization org = converter.convert(organization);
        
        
        // assert
        
        assertNotNull(org);
        
        assertEquals("Interdyscyplinary Centre", org.getName());
        assertEquals("ICM", org.getShortName());
        assertEquals("Poland", org.getCountryName());
        assertEquals("PL", org.getCountryCode());
        assertEquals("www.icm.edu.pl", org.getWebsiteUrl());
        
        verifyZeroInteractions(log);
        
    }

    
    //------------------------ PRIVATE --------------------------
    
    private eu.dnetlib.dhp.schema.oaf.Organization createEmptyOafObject() {

        return createOafObject(null, null, null, null);
        
    }
    
    private eu.dnetlib.dhp.schema.oaf.Organization createOafObject(Qualifier country, Field<String> legalname, Field<String> legalshortname,
            Field<String> websiteurl) {
        
        eu.dnetlib.dhp.schema.oaf.Organization organization = new eu.dnetlib.dhp.schema.oaf.Organization();
        organization.setId("SOME_ID");
        organization.setCountry(country);
        organization.setLegalname(legalname);
        organization.setLegalshortname(legalshortname);
        organization.setWebsiteurl(websiteurl);
        
        return organization;
        
    }



    private Field<String> createStringField(String value) {
        Field<String> field = new Field<String>();
        field.setValue(value);
        return field;
    }
    
    
}
