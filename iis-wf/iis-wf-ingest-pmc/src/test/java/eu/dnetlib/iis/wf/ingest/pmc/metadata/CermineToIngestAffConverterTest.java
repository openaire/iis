package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;

/**
* @author Łukasz Dumiszewski
*/

public class CermineToIngestAffConverterTest {

    
    private CermineToIngestAffConverter cermineToIngestAffConverter = new CermineToIngestAffConverter();
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL() {
        
        // execute
        cermineToIngestAffConverter.convert(null);
    }

    
    @Test
    public void convert() {
        
        // given
        
        CermineAffiliation cAff = new CermineAffiliation();
        cAff.setAddress("some address");
        cAff.setCountryCode("PL");
        cAff.setCountryName("Poland");
        cAff.setInstitution("ICM, UW");
        cAff.setRawText("ICM, UW, Poland, some address");
        
        
        // execute
        
        Affiliation aff = cermineToIngestAffConverter.convert(cAff);
        
        
        // assert
        
        assertEquals(cAff.getAddress(), aff.getAddress());
        assertEquals(cAff.getCountryCode(), aff.getCountryCode());
        assertEquals(cAff.getCountryName(), aff.getCountryName());
        assertEquals(cAff.getInstitution(), aff.getOrganization());
        assertEquals(cAff.getRawText(), aff.getRawText());
        
    }

}

