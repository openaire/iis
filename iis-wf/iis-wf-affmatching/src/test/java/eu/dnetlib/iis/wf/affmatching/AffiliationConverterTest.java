package eu.dnetlib.iis.wf.affmatching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.beust.jcommander.internal.Lists;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffiliationConverterTest {

    @InjectMocks
    private AffiliationConverter converter = new AffiliationConverter();
    
    @Mock
    private StringNormalizer organizationNameNormalizer;
    
    @Mock
    private StringNormalizer countryNameNormalizer;
   
    @Mock
    private StringNormalizer countryCodeNormalizer;
    
    
    
    @Before
    public void before() {
        
        MockitoAnnotations.initMocks(this);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_null_document() {
        
        // execute
        
        converter.convert(null);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_organization_id() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId(null);
        
        
        // execute
        
        converter.convert(document);
        
    }
    
    
    @Test
    public void convert_null_affiliations() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId("xxx");
        assertNull(document.getAffiliations());

        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertTrue(affMatchAffiliations.size() == 0);
    }
    
    
    @Test
    public void convert_empty_affiliations() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId("ABC");
        document.setAffiliations(new ArrayList<>());

        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertTrue(affMatchAffiliations.size() == 0);
    }
    
    
    @Test
    public void convert_null_aff_properties() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId("XYZ");
        
        Affiliation aff1 = createAffiliation();
        aff1.setOrganization(null);
        aff1.setCountryName(null);
        aff1.setCountryCode(null);
        
        when(organizationNameNormalizer.normalize("")).thenReturn("X");
        when(countryNameNormalizer.normalize("")).thenReturn("X");
        when(countryCodeNormalizer.normalize("")).thenReturn("X");
        
        List<Affiliation> affiliations = Lists.newArrayList(aff1);
        document.setAffiliations(affiliations);

        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertAffiliation(affMatchAffiliations.get(0), "XYZ", 1, "X", "X", "X");
        
    }

    
    
    @Test
    public void convert() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId("XYZ");
        
        Affiliation aff1 = createAffiliation();
        Affiliation aff2 = createAffiliation();
        Affiliation aff3 = createAffiliation();
        
        when(organizationNameNormalizer.normalize(aff1.getOrganization().toString())).thenReturn("abc inst name");
        when(organizationNameNormalizer.normalize(aff2.getOrganization().toString())).thenReturn("def inst name");
        when(organizationNameNormalizer.normalize(aff3.getOrganization().toString())).thenReturn("ghi inst name");
        
        when(countryNameNormalizer.normalize(aff1.getCountryName().toString())).thenReturn("Poland");
        when(countryNameNormalizer.normalize(aff2.getCountryName().toString())).thenReturn("Germany");
        when(countryNameNormalizer.normalize(aff3.getCountryName().toString())).thenReturn("Slovakia");
        
        when(countryCodeNormalizer.normalize(aff1.getCountryCode().toString())).thenReturn("pl");
        when(countryCodeNormalizer.normalize(aff2.getCountryCode().toString())).thenReturn("de");
        when(countryCodeNormalizer.normalize(aff3.getCountryCode().toString())).thenReturn("sl");
        
        List<Affiliation> affiliations = Lists.newArrayList(aff1, aff2, aff3);
        document.setAffiliations(affiliations);

        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertAffiliation(affMatchAffiliations.get(0), "XYZ", 1, "abc inst name", "Poland", "pl");
        assertAffiliation(affMatchAffiliations.get(1), "XYZ", 2, "def inst name", "Germany", "de");
        assertAffiliation(affMatchAffiliations.get(2), "XYZ", 3, "ghi inst name", "Slovakia", "sl");
        
    }

    
    
    //------------------------ PRIVATE --------------------------

    private Affiliation createAffiliation() {
        Affiliation aff = new Affiliation();
        aff.setOrganization(RandomStringUtils.randomAlphabetic(20));
        aff.setCountryCode(RandomStringUtils.randomAlphabetic(2));
        aff.setCountryName(RandomStringUtils.randomAlphabetic(10));
        return aff;
    }
    

    private void assertAffiliation(AffMatchAffiliation actualAffMatchAff, String expectedDocId, int expectedPosition, String expectedOrgName, String expectedCountryName, String expectedCountryCode) {
        assertEquals(expectedDocId, actualAffMatchAff.getDocumentId());
        assertEquals((long)expectedPosition, (long)actualAffMatchAff.getPosition());
        assertEquals(expectedOrgName, actualAffMatchAff.getOrganizationName());
        assertEquals(expectedCountryName, actualAffMatchAff.getCountryName());
        assertEquals(expectedCountryCode, actualAffMatchAff.getCountryCode());

    }
    
}
