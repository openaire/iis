package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.iis.importer.schemas.Service;

/**
* @author Marek Horst
*/

public class ServiceConverterTest {

	private static String eligibleCollectedFromDatasourceId = "eligible-ds-id";
    private ServiceConverter converter = new ServiceConverter(eligibleCollectedFromDatasourceId);
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void buildObject_resolvedOafObject_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
        
    }

    @Test
    public void buildObject() throws Exception {
        
        //given
    	String id = "someId";
    	String serviceName = "Cermine";
    	String serviceUrl = "cermine.ceon.pl";
    	
        eu.dnetlib.dhp.schema.oaf.Datasource datasource = createOafObject(
        		id,
        		createStringField(serviceName),
                createStringField(serviceUrl));
        
        // execute 
        
        Service service = converter.convert(datasource);
        
        
        // assert
        
        assertNotNull(service);
        
        assertEquals(id, service.getId());
        assertEquals(serviceName, service.getName());
        assertEquals(serviceUrl, service.getUrl());
        
        
    }

    @Test
    public void buildObject_website_null() throws Exception {
        
    	 //given
    	String id = "someId";
    	String serviceName = "Cermine";
    	String serviceUrl = null;
    	
        eu.dnetlib.dhp.schema.oaf.Datasource datasource = createOafObject(
        		id,
        		createStringField(serviceName),
                createStringField(serviceUrl));
        
        // execute 
        
        Service service = converter.convert(datasource);
        
        
        // assert
        
        assertNull(service);
    }
    
    @Test
    public void buildObject_name_null() throws Exception {
        
    	 //given
    	String id = "someId";
    	String serviceName = null;
    	String serviceUrl = "cermine.ceon.pl";
    	
        eu.dnetlib.dhp.schema.oaf.Datasource datasource = createOafObject(
        		id,
        		createStringField(serviceName),
                createStringField(serviceUrl));
        
        // execute 
        
        Service service = converter.convert(datasource);
        
        
        // assert
        
        assertNotNull(service);
        
        assertEquals(id, service.getId());
        assertNull(service.getName());
        assertEquals(serviceUrl, service.getUrl());
    }
    
    
    @Test
    public void buildObject_not_a_service() throws Exception {
        
    	 //given
    	String id = "someId";
    	String serviceName = null;
    	String serviceUrl = null;
    	
        eu.dnetlib.dhp.schema.oaf.Datasource datasource = createOafObject(
        		id,
        		createStringField(serviceName),
                createStringField(serviceUrl),
                "other-ds-id");
        
        // execute 
        
        Service service = converter.convert(datasource);
        
        
        // assert
        
        assertNull(service);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private eu.dnetlib.dhp.schema.oaf.Datasource createOafObject(String id, Field<String> name, Field<String> websiteurl) {

    	return createOafObject(id, name, websiteurl, eligibleCollectedFromDatasourceId);
        
    }
    
	private eu.dnetlib.dhp.schema.oaf.Datasource createOafObject(String id, Field<String> name,
			Field<String> websiteurl, String collectedFromId) {
    
        eu.dnetlib.dhp.schema.oaf.Datasource datasource = new eu.dnetlib.dhp.schema.oaf.Datasource();

        if (collectedFromId != null) {
        	List<KeyValue> collectedFrom = new ArrayList<KeyValue>();
        	KeyValue dsId = new KeyValue();
        	dsId.setKey(collectedFromId);
        	collectedFrom.add(dsId);
        	datasource.setCollectedfrom(collectedFrom);
        }
        
        datasource.setId(id);
        datasource.setOfficialname(name);
        datasource.setWebsiteurl(websiteurl);
        
        return datasource;
        
    }

    private Field<String> createStringField(String value) {
        Field<String> field = new Field<String>();
        field.setValue(value);
        return field;
    }
    
}
