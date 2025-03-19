package eu.dnetlib.iis.wf.metadataextraction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;

public class TeiToExtractedDocumentMetadataTransformerTest {

    private String sampleTeiXml;

    @BeforeEach
    public void setUp() throws Exception {
        // Load sample TEI XML file from resources
        try (InputStream inputStream = getClass().getResourceAsStream("/eu/dnetlib/iis/wf/metadataextraction/grobid/input/sample1.tei.xml")) {
            assertNotNull(inputStream, "Sample TEI XML file not found in resources");
            sampleTeiXml = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testTransformWithMinimalContent() throws Exception {
        // Given
        String documentId = "minimal-doc";
        String minimalTei = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<TEI xmlns=\"http://www.tei-c.org/ns/1.0\">\n" +
                "<div xmlns=\"http://www.tei-c.org/ns/1.0\">\n" +
                "<p>This is a minimal test document.</p>\n" +
                "</div>\n" +
                "</TEI>";

        // When
        ExtractedDocumentMetadata metadata = TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, minimalTei);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        assertEquals("This is a minimal test document.\n\n", metadata.getText(), "Text content should match the provided content");
        assertEquals("This is a minimal test document.", metadata.getAbstract$(), "Abstract content should match the provided content");
        assertNull(metadata.getTitle(), "Title should be null for minimal content");
        assertNull(metadata.getYear(), "Year should be null for minimal content");
        assertNull(metadata.getIssue(), "Issue should be null for minimal content");
        assertNull(metadata.getJournal(), "Journal should be null for minimal content");
        assertNull(metadata.getLanguage(), "Language should be null for minimal content");
        assertNull(metadata.getVolume(), "Volume should be null for minimal content");
        assertNull(metadata.getPublisher(), "Publisher should be null for minimal content");
        assertNull(metadata.getPublicationTypeName(), "Publication type name should be null for minimal content");
        assertNull(metadata.getPages(), "Pages should be null for minimal content");
        assertNull(metadata.getAuthors(), "Authors should be null for minimal content");
        assertNull(metadata.getKeywords(), "Keywords should be null for minimal content");
        assertNull(metadata.getExternalIdentifiers(), "External identifiers should be null for minimal content");
        assertNull(metadata.getAffiliations(), "Affiliations should be null for minimal content");
        assertNull(metadata.getReferences(), "References should be null for minimal content");
    }

    @Test
    public void testExtractionWithCustomTei() throws Exception {
        // Given
        String documentId = "custom-doc";
        String customTei = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<TEI xmlns=\"http://www.tei-c.org/ns/1.0\">\n" +
                "<teiHeader>\n" +
                "  <fileDesc>\n" +
                "    <titleStmt>\n" +
                "      <title>Test Document Title</title>\n" +
                "    </titleStmt>\n" +
                "    <publicationStmt>\n" +
                "      <date when=\"2022\">2022</date>\n" +
                "      <publisher>Test Publisher</publisher>\n" +
                "    </publicationStmt>\n" +
                "  </fileDesc>\n" +
                "  <profileDesc>\n" +
                "    <abstract>\n" +
                "      <p>This is a test abstract.</p>\n" +
                "    </abstract>\n" +
                "    <langUsage>\n" +
                "      <language ident=\"en\">English</language>\n" +
                "    </langUsage>\n" +
                "  </profileDesc>\n" +
                "</teiHeader>\n" +
                "<div xmlns=\"http://www.tei-c.org/ns/1.0\">\n" +
                "<p>This is the main content of the test document.</p>\n" +
                "</div>\n" +
                "</TEI>";

        // When
        ExtractedDocumentMetadata metadata = TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, customTei);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        assertEquals("Test Document Title", metadata.getTitle(), "Title should be correctly extracted");
        assertEquals(Integer.valueOf(2022), metadata.getYear(), "Year should be correctly extracted");
        assertEquals("Test Publisher", metadata.getPublisher(), "Publisher should be correctly extracted");
        assertEquals("This is a test abstract.", metadata.getAbstract$(), "Abstract should be correctly extracted");
        assertEquals("en", metadata.getLanguage(), "Language should be correctly extracted");
        assertEquals("Test Document Title\n"
                + "\n"
                + "ABSTRACT\n"
                + "This is a test abstract.\n"
                + "\n"
                + "This is the main content of the test document.\n\n", metadata.getText(), "Text content should be correctly extracted");
    }
    
    @Test
    public void testExceptionHandlingForInvalidXML() {
        // Given
        String documentId = "invalid-doc";
        String invalidXml = "This is not valid XML";
        
        // When & Then
        assertThrows(
            Exception.class,
            () -> TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, invalidXml),
            "Should throw an exception for invalid XML"
        );
    }
    
    @Test
    public void testExceptionHandlingForMissingId() {
        // Given
        String documentId = null;
        
        // When & Then
        assertThrows(
            Exception.class,
            () -> TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, sampleTeiXml),
            "Should throw an exception for invalid XML"
        );
    }
    
    @Test
    public void testTransformSample1TeiXml() throws Exception {
        // Given
        String documentId = "test-doc-id-123";

        // When
        ExtractedDocumentMetadata metadata = TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, sampleTeiXml);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        
        // Verify title
        assertEquals("Stressful life events are not associated with the development of dementia", metadata.getTitle(), 
                "Title should match the content in the TEI file");
        
        // Verify journal
        assertEquals("International Psychogeriatrics", metadata.getJournal(), 
                "Journal should match the content in the TEI file");
        
        // Verify publication year
        assertEquals(Integer.valueOf(2014), metadata.getYear(), 
                "Year should match the content in the TEI file");
        
        // Verify publisher
        assertEquals("Elsevier BV", metadata.getPublisher(), 
                "Publisher should match the content in the TEI file");
        
        // Verify volume and issue
        assertEquals("26", metadata.getVolume(), "Volume should match the content in the TEI file");
        assertEquals("1", metadata.getIssue(), "Issue should match the content in the TEI file");
        
        // Verify pages
        assertNotNull(metadata.getPages(), "Pages should not be null");
        assertEquals("147", metadata.getPages().getStart(), "Start page should match the content in the TEi file");
        assertEquals("154", metadata.getPages().getEnd(), "End page should match the content in the TEI file");
        
        // Verify authors
        List<Author> authors = metadata.getAuthors();
        assertNotNull(authors, "Authors should not be null");
        assertEquals(4, authors.size(), "Should extract 4 authors from the sample file");
        
        Author author = authors.get(0);
        assertEquals("Anna Sundström", author.getAuthorFullName());
        assertNotNull(author.getAffiliationPositions());
        assertEquals(0, author.getAffiliationPositions().get(0));

        author = authors.get(1);
        assertEquals("Michael Rönnlund", author.getAuthorFullName());
        assertNotNull(author.getAffiliationPositions());
        assertEquals(1, author.getAffiliationPositions().get(0));

        author = authors.get(2);
        assertEquals("Rolf Adolfsson", author.getAuthorFullName());
        assertNotNull(author.getAffiliationPositions());
        assertEquals(2, author.getAffiliationPositions().get(0));

        author = authors.get(3);
        assertEquals("Lars-Göran Nilsson", author.getAuthorFullName());
        assertNotNull(author.getAffiliationPositions());
        assertEquals(3, author.getAffiliationPositions().get(0));

        
        // Verify affiliations
        List<Affiliation> affiliations = metadata.getAffiliations();
        assertNotNull(affiliations, "Affiliations should not be null");
        assertFalse(affiliations.isEmpty(), "Affiliations should not be empty");
        assertEquals(4, affiliations.size());
        
        // Verifying all the fields for the first affiliation
        assertEquals("Centre for Population Studies, Department of Psychology, Ageing and Living Conditions, Umeå University", affiliations.get(0).getOrganization());
        assertEquals("Umeå, Sweden", affiliations.get(0).getAddress());
        assertEquals("Sweden", affiliations.get(0).getCountryName());
        assertEquals("SE", affiliations.get(0).getCountryCode());
        assertEquals(
                "Centre for Population Studies/ Ageing and Living Conditions , and Department of Psychology , Umeå University , Umeå , Sweden",
                affiliations.get(0).getRawText());
        
        assertEquals("Department of Psychology, Umeå University", affiliations.get(1).getOrganization());
        assertEquals("Umeå, Sweden", affiliations.get(1).getAddress());
        assertEquals("Sweden", affiliations.get(1).getCountryName());
        assertEquals("SE", affiliations.get(1).getCountryCode());
        assertEquals(
                "Department of Psychology , Umeå University , Umeå , Sweden",
                affiliations.get(1).getRawText());
        
        assertEquals("Department of Clinical Sciences, Division of Psychiatry, Umeå University", affiliations.get(2).getOrganization());
        assertEquals("Umeå, Sweden", affiliations.get(2).getAddress());
        assertEquals("Sweden", affiliations.get(2).getCountryName());
        assertEquals("SE", affiliations.get(2).getCountryCode());
        assertEquals(
                "Department of Clinical Sciences , Division of Psychiatry , Umeå University , Umeå , Sweden",
                affiliations.get(2).getRawText());
        
        assertEquals("Department of Psychology, Stockholm University, Stockholm Brain Institute", affiliations.get(3).getOrganization());
        assertEquals("Stockholm, Sweden", affiliations.get(3).getAddress());
        assertEquals("Sweden", affiliations.get(3).getCountryName());
        assertEquals("SE", affiliations.get(3).getCountryCode());
        assertEquals(
                "Department of Psychology , Stockholm University , and Stockholm Brain Institute , Stockholm , Sweden",
                affiliations.get(3).getRawText());
        
        // Verify abstract contains the correct content
        String abstractText = metadata.getAbstract$().toString();
        assertTrue(abstractText.contains("Background:"), "Abstract should contain 'Background' section");
        assertTrue(abstractText.contains("Methods:"), "Abstract should contain 'Methods' section");
        assertTrue(abstractText.contains("Conclusions:"), "Abstract should contain 'Conclusions' section");
        assertTrue(abstractText.contains("stressful life events"), "Abstract should contain key terms from the paper");
        
        // Verify keywords
        List<CharSequence> keywords = metadata.getKeywords();
        assertNotNull(keywords, "Keywords should not be null");
        assertTrue(keywords.size() >= 5, "Should extract at least 5 keywords");
        
        // Check specific keywords
        boolean foundDementia = false;
        boolean foundStress = false;
        for (CharSequence keyword : keywords) {
            String keywordStr = keyword.toString();
            if (keywordStr.equals("dementia")) {
                foundDementia = true;
            } else if (keywordStr.equals("stress")) {
                foundStress = true;
            }
        }
        assertTrue(foundDementia, "Should find keyword 'dementia'");
        assertTrue(foundStress, "Should find keyword 'stress'");
        
        // Verify references
        List<ReferenceMetadata> references = metadata.getReferences();
        assertNotNull(references, "References should not be null");
        assertFalse(references.isEmpty(), "References should not be empty");
        
        // Check that text content is not empty
        assertNotNull(metadata.getText(), "Text content should not be null");
        assertTrue(metadata.getText().length() > 1000, "Text content should have significant length");
    }

    @Test
    public void testTransformSample1TeiXmlFocusingOnReferences() throws Exception {
        // Given
        String documentId = "ref-test";
        
        // When
        ExtractedDocumentMetadata metadata = TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, sampleTeiXml);
        
        // Then - verify references
        List<ReferenceMetadata> references = metadata.getReferences();
        assertNotNull(references, "References should not be null");
        assertFalse(references.isEmpty(), "References should not be empty");
        
        // Verify we extracted at least 20 references
        assertTrue(references.size() >= 20, "Should extract at least 20 references, found: " + references.size());
        
        // Check for specific references from the sample
        boolean foundBaumeisterReference = false;
        boolean foundFratiglioni = false;
        boolean foundGreenReference = false;
        
//      FIXME add more detailed checks
        
        for (ReferenceMetadata reference : references) {
            String text = reference.getText().toString().toLowerCase();
            
            if (text.contains("baumeister") && text.contains("bad is stronger than good")) {
                foundBaumeisterReference = true;
            } else if (text.contains("fratiglioni") && text.contains("active and socially integrated lifestyle")) {
                foundFratiglioni = true;
            } else if (text.contains("green") && text.contains("glucocorticoids increase amyloid")) {
                foundGreenReference = true;
            }
            
            // Verify the reference has basic structure
            assertNotNull(reference.getText(), "Reference text should not be null");
            assertTrue(reference.getText().length() > 10, "Reference text should have reasonable length");
            assertNotNull(reference.getPosition(), "Reference should have position");
        }
        
        assertTrue(foundBaumeisterReference, "Should find reference to Baumeister paper about 'Bad is stronger than good'");
        assertTrue(foundFratiglioni, "Should find reference to Fratiglioni paper about 'active lifestyle'");
        assertTrue(foundGreenReference, "Should find reference to Green paper about 'glucocorticoids'");
    }
}
