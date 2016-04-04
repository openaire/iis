package eu.dnetlib.iis.wf.importer.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OrganizationProtos.Organization.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.importer.schemas.Organization;

/**
* @author Łukasz Dumiszewski
*/

public class OrganizationConverterTest {

    private OrganizationConverter converter = new OrganizationConverter();
    
    private Result result = mock(Result.class);
    
    private Logger log = mock(Logger.class);
    
    
    @Before
    public void before() {
        
        Whitebox.setInternalState(OrganizationConverter.class, "log", log);
        
    }
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void buildObject_resolvedOadObject_NULL() throws Exception {
        
        // execute
        
        converter.buildObject(result, null);
        
    }
    
    

    @Test(expected = NullPointerException.class)
    public void buildObject_source_NULL() throws Exception {
        
        // given
        
        Oaf oaf = Oaf.newBuilder().setKind(Kind.entity).build();
        
        // execute
        
        converter.buildObject(null, oaf);
        
    }
    
    
    
    @Test
    public void buildObject_organization_metadata_name_empty() throws Exception {
        
        //given
        
        Metadata orgMetadata = Metadata.newBuilder().build();
        Oaf oaf = createOafObject(orgMetadata);
        
        // execute & assert
        
        assertNull(converter.buildObject(result, oaf));
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
        Oaf oaf = createOafObject(orgMetadata);
        
        
        // execute 
        
        Organization org = converter.buildObject(result, oaf);
        
        
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
    
    private Oaf createOafObject(Metadata orgMetadata) {
        
        eu.dnetlib.data.proto.OrganizationProtos.Organization organization = eu.dnetlib.data.proto.OrganizationProtos.Organization.newBuilder()
                                    .setMetadata(orgMetadata)
                                    .build();
        
        OafEntity oafEntity = OafEntity.newBuilder().setType(Type.organization).setId("SOME_ID").setOrganization(organization).build();
        
        Oaf oaf = Oaf.newBuilder().setKind(Kind.entity).setEntity(oafEntity).build();
        
        return oaf;
    }



    private StringField createStringField(String value) {
        return StringField.newBuilder().setValue(value).build();
    }
    
    
}
