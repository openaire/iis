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

import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OrganizationProtos.Organization.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.importer.infospace.converter.OrganizationConverter;

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
        
        Metadata orgMetadata = Metadata.newBuilder().build();
        OafEntity oafEntity = createOafObject(orgMetadata);
        
        // execute & assert
        
        assertNull(converter.convert(oafEntity));
        verify(log).error("skipping: empty organization name");
        
    }

    @Test
    public void buildObject() throws Exception {
        
        //given
        
        Qualifier country = Qualifier.newBuilder().setClassid("PL").setClassname("Poland").build();
        Metadata orgMetadata = Metadata.newBuilder()
                                            .setLegalname(createStringField("Interdyscyplinary Centre"))
                                            .setLegalshortname(createStringField("ICM"))
                                            .setCountry(country)
                                            .setWebsiteurl(createStringField("www.icm.edu.pl"))
                                            .build();
        OafEntity oafEntity = createOafObject(orgMetadata);
        
        
        // execute 
        
        Organization org = converter.convert(oafEntity);
        
        
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
    
    private OafEntity createOafObject(Metadata orgMetadata) {
        
        eu.dnetlib.data.proto.OrganizationProtos.Organization organization = eu.dnetlib.data.proto.OrganizationProtos.Organization.newBuilder()
                                    .setMetadata(orgMetadata)
                                    .build();
        
        return OafEntity.newBuilder().setType(Type.organization).setId("SOME_ID").setOrganization(organization).build();
        
    }



    private StringField createStringField(String value) {
        return StringField.newBuilder().setValue(value).build();
    }
    
    
}
