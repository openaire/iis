package eu.dnetlib.iis.wf.metadataextraction;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TeiToExtractedDocumentMetadataTransformerTest {

    private TeiToExtractedDocumentMetadataTransformer transformer;
    private String sampleTeiXml;

    @BeforeEach
    public void setUp() throws Exception {
        transformer = new TeiToExtractedDocumentMetadataTransformer();
        // Load sample TEI XML file from resources
        try (InputStream inputStream = getClass().getResourceAsStream("/eu/dnetlib/iis/wf/metadataextraction/grobid/input/sample1.tei.xml")) {
            assertNotNull(inputStream, "Sample TEI XML file not found in resources");
            sampleTeiXml = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testTransformSample1TeiXml() throws Exception {
        // Given
        String documentId = "test-doc-id-123";

        // When
        ExtractedDocumentMetadata metadata = transformer.transformToExtractedDocumentMetadata(documentId, sampleTeiXml);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        
        // Verify text content extraction
        assertNotNull(metadata.getText(), "Text content should not be null");
        assertTrue(metadata.getText().length() > 100, "Text content should have significant length");
        assertTrue(((String) metadata.getText()).contains("This paper explores the power of personality traits"), "Text should contain content from the TEI document");
        
        // Verify basic metadata extraction
        assertNotNull(metadata.getAbstract$(), "Abstract should not be null");
        assertTrue(((String) metadata.getAbstract$()).contains("This paper explores the power of personality traits"), "Abstract should contain expected content");
        
        // Verify references extraction
        List<ReferenceMetadata> references = metadata.getReferences();
        assertNotNull(references, "References should not be null");
        assertFalse(references.isEmpty(), "References should not be empty");
        
        // Check a few references to ensure they're extracted properly
        boolean foundReference = false;
        for (ReferenceMetadata reference : references) {
            assertNotNull(reference.getText(), "Reference text should not be null");
            if (reference.getBasicMetadata() != null && 
                reference.getBasicMetadata().getTitle() != null && 
                reference.getBasicMetadata().getTitle().toString().contains("Bell Curve")) {
                foundReference = true;
                break;
            }
        }
        assertTrue(foundReference, "Should find at least one reference to 'The Bell Curve'");
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
        ExtractedDocumentMetadata metadata = transformer.transformToExtractedDocumentMetadata(documentId, minimalTei);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        assertEquals("This is a minimal test document.", metadata.getText(), "Text content should match the provided content");
        assertNull(metadata.getTitle(), "Title should be null for minimal content");
        assertNull(metadata.getYear(), "Year should be null for minimal content");
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
        ExtractedDocumentMetadata metadata = transformer.transformToExtractedDocumentMetadata(documentId, customTei);

        // Then
        assertNotNull(metadata, "Extracted metadata should not be null");
        assertEquals(documentId, metadata.getId(), "Document ID should match the provided ID");
        assertEquals("Test Document Title", metadata.getTitle(), "Title should be correctly extracted");
        assertEquals(Integer.valueOf(2022), metadata.getYear(), "Year should be correctly extracted");
        assertEquals("Test Publisher", metadata.getPublisher(), "Publisher should be correctly extracted");
        assertEquals("This is a test abstract.", metadata.getAbstract$(), "Abstract should be correctly extracted");
        assertEquals("en", metadata.getLanguage(), "Language should be correctly extracted");
        assertEquals("This is the main content of the test document.", metadata.getText(), "Text content should be correctly extracted");
    }

    @Test
    public void testDetailedExtractionsFromSample1() throws Exception {
        // When
        ExtractedDocumentMetadata metadata = transformer.transformToExtractedDocumentMetadata("detailed-test", sampleTeiXml);

        // Then - Check authors
        List<Author> authors = metadata.getAuthors();
        assertNotNull(authors, "Authors should not be null");
        assertFalse(authors.isEmpty(), "Authors should not be empty");
        
        // The sample has mentions of various authors like "Allport", "Binet", etc.
        boolean foundAuthor = false;
        for (Author author : authors) {
            if (author.getAuthorFullName() != null && 
                (author.getAuthorFullName().toString().contains("Allport") || 
                 author.getAuthorFullName().toString().contains("Binet"))) {
                foundAuthor = true;
                break;
            }
        }
        assertTrue(foundAuthor, "Should find at least one expected author");
        
        // Check affiliations
        List<Affiliation> affiliations = metadata.getAffiliations();
        if (affiliations != null && !affiliations.isEmpty()) {
            // If affiliations are extracted, verify they have expected properties
            for (Affiliation affiliation : affiliations) {
                assertNotNull(affiliation, "Affiliation should not be null");
                // At least one of these fields should be populated
                boolean hasData = affiliation.getOrganization() != null || 
                                  affiliation.getCountryName() != null || 
                                  affiliation.getAddress() != null;
                assertTrue(hasData, "Affiliation should have at least one populated field");
            }
        }
        
        // Check for keywords if present
        if (metadata.getKeywords() != null) {
            assertFalse(metadata.getKeywords().isEmpty(), "Keywords should not be empty if present");
        }
        
        // Verify publication type if present
        if (metadata.getPublicationTypeName() != null) {
            assertFalse(metadata.getPublicationTypeName().toString().isEmpty(), 
                    "Publication type name should not be empty if present");
        }
    }
    
    @Test
    public void testExceptionHandling() {
        // Given
        String documentId = "invalid-doc";
        String invalidXml = "This is not valid XML";
        
        // When & Then
        Exception exception = assertThrows(
            Exception.class,
            () -> transformer.transformToExtractedDocumentMetadata(documentId, invalidXml),
            "Should throw an exception for invalid XML"
        );
        
        assertTrue(exception.getMessage().contains("XML") || 
                  exception.getCause().getMessage().contains("XML"), 
                  "Exception should mention XML parsing issue");
    }
}
