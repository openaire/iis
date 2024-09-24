package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * {@link JatsExtendedAffiliation} test class. 
 * @author mhorst
 */
public class JatsExtendedAffiliationTest {
    
    private final String institutionOrgDivision = "ICM"; 
    private final String institutionOrgName = "University of Warsaw"; 
    private final String addrLineStreet = "Tyniecka 15/17";
    private final String addrLinePostCode = "02-630"; 
    private final String addrLineCity = "Warsaw"; 
    private final String countryName = "Poland";
    private final String countryCode = "PL";

    // -------------------------------TEST------------------------------
    
    @Test
    public void testClear() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), institutionOrgDivision, institutionOrgName, 
                addrLineStreet, addrLinePostCode, addrLineCity, 
                countryName, countryCode);
        
        // execute
        aff.clear();
        
        // assert
        assertEquals("", aff.getInstitutionOrgDivision());
        assertEquals("", aff.getInstitutionOrgName());
        assertEquals("", aff.getAddrLineStreet());
        assertEquals("", aff.getAddrLinePostCode());
        assertEquals("", aff.getAddrLineCity());
        assertEquals("", aff.getCountryName());
        assertEquals("", aff.getCountryCode());
        assertEquals(0, aff.getNumberOfFieldsSet());
    }
    
    
    @Test
    public void testGenerateFullOrganizationName() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), institutionOrgDivision, institutionOrgName, 
                null, null, null, null, null);
        
        // execute & assert
        assertEquals("ICM, University of Warsaw", aff.generateFullOrganizationName());
        assertEquals(2, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateFullOrganizationNameWithoutDivision() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), null, institutionOrgName, 
                null, null, null, null, null);
        
        // execute & assert
        assertEquals("University of Warsaw", aff.generateFullOrganizationName());
        assertEquals(1, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateFullOrganizationNameWithoutName() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), institutionOrgDivision, null, 
                null, null, null, null, null);
        
        // execute & assert
        assertEquals("ICM", aff.generateFullOrganizationName());
        assertEquals(1, aff.getNumberOfFieldsSet());
    }
     
    @Test
    public void testGenerateFullAddress() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), null, null, 
                addrLineStreet, addrLinePostCode, addrLineCity, null, null);
        
        // execute & assert
        assertEquals("Tyniecka 15/17, 02-630 Warsaw", aff.generateFullAddress());
        assertEquals(3, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateFullAddressWithoutPostCode() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), null, null, 
                addrLineStreet, null, addrLineCity, null, null);
        
        // execute & assert
        assertEquals("Tyniecka 15/17, Warsaw", aff.generateFullAddress());
        assertEquals(2, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateFullAddressWithoutStreet() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), null, null, 
                null, addrLinePostCode, addrLineCity, null, null);
        
        // execute & assert
        assertEquals("02-630 Warsaw", aff.generateFullAddress());
        assertEquals(2, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateFullAddressWithoutCity() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), null, null, 
                addrLineStreet, addrLinePostCode, null, null, null);
        
        // execute & assert
        assertEquals("Tyniecka 15/17, 02-630", aff.generateFullAddress());
        assertEquals(2, aff.getNumberOfFieldsSet());
    }

    @Test
    public void testGenerateRawText() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), institutionOrgDivision, institutionOrgName, 
                addrLineStreet, addrLinePostCode, addrLineCity, 
                countryName, countryCode);
        
        // execute & assert
        assertEquals("ICM, University of Warsaw, Tyniecka 15/17, 02-630 Warsaw, Poland", aff.generateRawText());
        assertEquals(7, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testGenerateRawTextNoCountryName() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(
                new JatsExtendedAffiliation(), institutionOrgDivision, institutionOrgName, 
                addrLineStreet, addrLinePostCode, addrLineCity, 
                null, countryCode);
        
        // execute & assert
        assertEquals("ICM, University of Warsaw, Tyniecka 15/17, 02-630 Warsaw", aff.generateRawText());
        assertEquals(6, aff.getNumberOfFieldsSet());
    }
    
    @Test
    public void testNoFieldSet() throws Exception {
        // given
        JatsExtendedAffiliation aff = fillJatsExtendedAffiliationWithMetadata(new JatsExtendedAffiliation(), null, null,
                null, null, null, null, null);
        
        // execute & assert
        assertEquals(0, aff.getNumberOfFieldsSet());
    }

    // ------------------------------ PRIVATE --------------------------------
    
    private static JatsExtendedAffiliation fillJatsExtendedAffiliationWithMetadata(JatsExtendedAffiliation aff,
            String institutionOrgDivision, String institutionOrgName, 
            String addrLineStreet, String addrLinePostCode, String addrLineCity, 
            String countryName, String countryCode) {
        aff.appendToInstitutionOrgDivision(institutionOrgDivision);
        aff.appendToInstitutionOrgName(institutionOrgName);

        aff.appendToAddrLineStreet(addrLineStreet);
        aff.appendToAddrLinePostCode(addrLinePostCode);
        aff.appendToAddrLineCity(addrLineCity);

        aff.appendToCountryName(countryName);
        aff.setCountryCode(countryCode);
        return aff;
    }
    
}
