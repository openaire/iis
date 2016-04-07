package eu.dnetlib.iis.wf.metadataextraction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;

/**
* @author Łukasz Dumiszewski
*/

public class CermineToMetadataAffConverterTest {

    
    private CermineToMetadataAffConverter cermineToMetadataAffConverter = new CermineToMetadataAffConverter();
    
    
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL() {
        
        // execute
        cermineToMetadataAffConverter.convert(null);
    }

    
    @Test
    public void convert() {
        
        // given
        
        CermineAffiliation cAff = mock(CermineAffiliation.class);
        when(cAff.getAddress()).thenReturn("some address");
        when(cAff.getCountryCode()).thenReturn("PL");
        when(cAff.getCountryName()).thenReturn("Poland");
        when(cAff.getInstitution()).thenReturn("ICM, UW");
        when(cAff.getRawText()).thenReturn("ICM, UW, Poland, some address");
        
                
        // execute
        
        Affiliation aff = cermineToMetadataAffConverter.convert(cAff);
        
        
        // assert
        
        assertEquals("some address", aff.getAddress());
        assertEquals("PL", aff.getCountryCode());
        assertEquals("Poland", aff.getCountryName());
        assertEquals("ICM, UW", aff.getOrganization());
        assertEquals("ICM, UW, Poland, some address", aff.getRawText());
        
        
    }

}
