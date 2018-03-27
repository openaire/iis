package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffiliationConverterTest {

    private AffiliationConverter converter = new AffiliationConverter();
    
    
   
    
    
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
        
        Affiliation aff1 = createAffiliation(null, null, null);
        
        
        List<Affiliation> affiliations = Lists.newArrayList(aff1);
        document.setAffiliations(affiliations);
        document.setAuthors(Lists.newArrayList(createAuthor("author1", 0)));

        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertAffiliation(affMatchAffiliations.get(0), "XYZ", 1, "", "", "");
        
    }

    
    
    @Test
    public void convert() {
        
        // given
        
        ExtractedDocumentMetadata document = new ExtractedDocumentMetadata();
        document.setId("XYZ");
        
        Affiliation aff1 = createAffiliation("ABC", "PL", "Poland");
        Affiliation aff2 = createAffiliation("DEF", "DE", "Deutschland");
        Affiliation aff3 = createAffiliation("GHI", "SL", "Slovakia");
        
        List<Affiliation> affiliations = Lists.newArrayList(aff1, aff2, aff3);
        document.setAffiliations(affiliations);
        document.setAuthors(Lists.newArrayList(createAuthor("author1", 0), createAuthor("author2", 1)));
        
        // execute
        
        List<AffMatchAffiliation> affMatchAffiliations = converter.convert(document);
        
        
        // assert
        
        assertNotNull(affMatchAffiliations);
        assertEquals(2, affMatchAffiliations.size());
        assertAffiliation(affMatchAffiliations.get(0), "XYZ", 1, "ABC", "Poland", "PL");
        assertAffiliation(affMatchAffiliations.get(1), "XYZ", 2, "DEF", "Deutschland", "DE");
        
    }

    
    
    //------------------------ PRIVATE --------------------------

    private Affiliation createAffiliation(String orgName, String countryCode, String countryName) {
        Affiliation aff = new Affiliation();
        aff.setOrganization(orgName);
        aff.setCountryCode(countryCode);
        aff.setCountryName(countryName);
        return aff;
    }
    
    private Author createAuthor(String fullName, int affiliationPosition) {
        Author author = new Author();
        author.setAuthorFullName(fullName);
        author.setAffiliationPositions(Arrays.asList(affiliationPosition));
        return author;
    }

    private void assertAffiliation(AffMatchAffiliation actualAffMatchAff, String expectedDocId, int expectedPosition, String expectedOrgName, String expectedCountryName, String expectedCountryCode) {
        assertEquals(expectedDocId, actualAffMatchAff.getDocumentId());
        assertEquals((long)expectedPosition, (long)actualAffMatchAff.getPosition());
        assertEquals(expectedOrgName, actualAffMatchAff.getOrganizationName());
        assertEquals(expectedCountryName, actualAffMatchAff.getCountryName());
        assertEquals(expectedCountryCode, actualAffMatchAff.getCountryCode());

    }
    
}
