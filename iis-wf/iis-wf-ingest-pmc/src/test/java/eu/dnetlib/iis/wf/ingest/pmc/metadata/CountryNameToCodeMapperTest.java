package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link CountryNameToCodeMapper} test class.
 * 
 * @author mhorst
 */
public class CountryNameToCodeMapperTest {

    private CountryNameToCodeMapper countryCodeMapper;

    @BeforeEach
    public void setUp() {
        countryCodeMapper = new CountryNameToCodeMapper();
    }

    @Test
    public void testGetCountryCode_WithDefaultMapping() {
        assertEquals("US", countryCodeMapper.getCountryCode("United States"));
        assertEquals("PL", countryCodeMapper.getCountryCode("Poland"));
        assertEquals("CA", countryCodeMapper.getCountryCode("CANADA"));
        assertEquals("AU", countryCodeMapper.getCountryCode("australia"));
    }

    @Test
    public void testGetCountryCode_WithAuxiliaryMapping() {
        assertEquals("US", countryCodeMapper.getCountryCode("USA"));
        assertEquals("US", countryCodeMapper.getCountryCode("usa"));
        assertEquals("KR", countryCodeMapper.getCountryCode("Republic of Korea"));
    }

    @Test
    public void testGetCountryCode_WithAuxiliaryMappingOverridingDefaultMapping() {
        countryCodeMapper = new CountryNameToCodeMapper(
                "eu/dnetlib/iis/wf/ingest/pmc/metadata/test_country_name_to_code.properties");
        assertNull(countryCodeMapper.getCountryCode("USA"));
        assertEquals("POL", countryCodeMapper.getCountryCode("POLAND"));
    }

    @Test
    public void testGetCountryCode_WithUnknownCountry() {
        assertNull(countryCodeMapper.getCountryCode("Unknown Country"));
    }

    @Test
    public void testGetCountryCode_WithNullInput() {
        assertNull(countryCodeMapper.getCountryCode(null));
    }

    @Test
    public void testGetCountryCode_WithEmptyInput() {
        assertNull(countryCodeMapper.getCountryCode(""));
    }
    
    @Test
    public void testInstantiate_ResourceFileNotFound() {
        assertThrows(RuntimeException.class, () -> new CountryNameToCodeMapper(
                "eu/dnetlib/iis/wf/ingest/pmc/metadata/non_existing.properties"));
    }
}
