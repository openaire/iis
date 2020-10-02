package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/

public class CermineToIngestAffConverterTest {

    
    private CermineToIngestAffConverter cermineToIngestAffConverter = new CermineToIngestAffConverter();
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void convert_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> cermineToIngestAffConverter.convert(null));
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
        
        Affiliation aff = cermineToIngestAffConverter.convert(cAff);
        
        
        // assert
        
        assertEquals("some address", aff.getAddress());
        assertEquals("PL", aff.getCountryCode());
        assertEquals("Poland", aff.getCountryName());
        assertEquals("ICM, UW", aff.getOrganization());
        assertEquals("ICM, UW, Poland, some address", aff.getRawText());
        
    }

}

