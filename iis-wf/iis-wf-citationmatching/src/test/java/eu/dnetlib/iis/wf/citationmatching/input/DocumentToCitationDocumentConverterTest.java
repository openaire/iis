package eu.dnetlib.iis.wf.citationmatching.input;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;

/**
 * @author madryk
 */
public class DocumentToCitationDocumentConverterTest {

    private DocumentToCitationDocumentConverter converter = new DocumentToCitationDocumentConverter();
    
    private final Set<Integer> emptyReferencePositions = Collections.emptySet();
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL_DOCUMENT() {
        
        // execute
        converter.convert(null, emptyReferencePositions);
    }
    
    @Test
    public void convert_NO_REFERENCES() {
        
        // given
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setTitle("Some Title")
                .setJournal("Journal Something")
                .setImportedAuthors(Lists.newArrayList(generateAuthor("author-1"), generateAuthor("author-2")))
                .setPages(new Range("5", "8"))
                .setYear(1999)
                .setReferences(Lists.newArrayList())
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, emptyReferencePositions);
        
        
        // assert
        
        DocumentMetadata expectedDocumentMetadata = DocumentMetadata.newBuilder()
                .setId("document-id")
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setAuthors(Lists.newArrayList("author-1", "author-2"))
                        .setTitle("Some Title")
                        .setJournal("Journal Something")
                        .setPages("5-8")
                        .setYear("1999")
                        .build())
                .setReferences(Lists.newArrayList())
                .build();
        
        assertEquals(expectedDocumentMetadata, retDocumentMetadata);
    }
    
    @Test
    public void convert_WITH_REFERENCES() {
        
        // given
        
        eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata inputReference1 = eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                .setPosition(1)
                .setBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                        .setTitle("First Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-3"))
                        .setSource("Other Journal")
                        .setPages(new Range("10", "11"))
                        .setYear("2005")
                        .build())
                .setText("reference 1 raw text")
                .build();
        
        eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata inputReference2 = eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                .setPosition(2)
                .setBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                        .setTitle("Second Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-1", "author-id-4"))
                        .setSource("Some Journal")
                        .setPages(new Range("10", "23"))
                        .setYear("2000")
                        .build())
                .setText("reference 2 raw text")
                .build();
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setImportedAuthors(Lists.newArrayList())
                .setReferences(Lists.newArrayList(inputReference1, inputReference2))
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, emptyReferencePositions);
        
        
        // assert
        
        assertEquals(2, retDocumentMetadata.getReferences().size());
        
        ReferenceMetadata expectedReference1 = ReferenceMetadata.newBuilder()
                .setPosition(1)
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setTitle("First Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-3"))
                        .setJournal("Other Journal")
                        .setPages("10-11")
                        .setYear("2005")
                        .build())
                .setRawText("reference 1 raw text")
                .build();
        
        assertEquals(expectedReference1, retDocumentMetadata.getReferences().get(0));
        
        
        ReferenceMetadata expectedReference2 = ReferenceMetadata.newBuilder()
                .setPosition(2)
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setTitle("Second Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-1", "author-id-4"))
                        .setJournal("Some Journal")
                        .setPages("10-23")
                        .setYear("2000")
                        .build())
                .setRawText("reference 2 raw text")
                .build();
        
        assertEquals(expectedReference2, retDocumentMetadata.getReferences().get(1));
        
    }
    
    @Test
    public void convert_WITH_FILTERED_REFERENCES() {
        
        // given
        
        eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata inputReference1 = eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                .setPosition(1)
                .setBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                        .setTitle("First Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-3"))
                        .setSource("Other Journal")
                        .setPages(new Range("10", "11"))
                        .setYear("2005")
                        .build())
                .setText("reference 1 raw text")
                .build();
        
        eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata inputReference2 = eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                .setPosition(2)
                .setBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                        .setTitle("Second Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-1", "author-id-4"))
                        .setSource("Some Journal")
                        .setPages(new Range("10", "23"))
                        .setYear("2000")
                        .build())
                .setText("reference 2 raw text")
                .build();
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setImportedAuthors(Lists.newArrayList())
                .setReferences(Lists.newArrayList(inputReference1, inputReference2))
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, Sets.newHashSet(2));
        
        
        // assert
        
        assertEquals(1, retDocumentMetadata.getReferences().size());
        
        ReferenceMetadata expectedReference1 = ReferenceMetadata.newBuilder()
                .setPosition(1)
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setTitle("First Reference Title")
                        .setAuthors(Lists.newArrayList("author-id-3"))
                        .setJournal("Other Journal")
                        .setPages("10-11")
                        .setYear("2005")
                        .build())
                .setRawText("reference 1 raw text")
                .build();
        
        assertEquals(expectedReference1, retDocumentMetadata.getReferences().get(0));
        
    }
    
    @Test
    public void convert_NULL_DOCUMENT_AUTHORS() {
        
        // given
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setImportedAuthors(null)
                .setReferences(Lists.newArrayList())
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, emptyReferencePositions);
        
        
        // assert
        
        DocumentMetadata expectedDocumentMetadata = DocumentMetadata.newBuilder()
                .setId("document-id")
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setAuthors(Lists.newArrayList())
                        .build())
                .setReferences(Lists.newArrayList())
                .build();
        
        assertEquals(expectedDocumentMetadata, retDocumentMetadata);
    }
    
    @Test
    public void convert_NULL_REFERENCES() {
        
        // given
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setImportedAuthors(Lists.newArrayList())
                .setReferences(null)
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, emptyReferencePositions);
        
        
        // assert
        
        DocumentMetadata expectedDocumentMetadata = DocumentMetadata.newBuilder()
                .setId("document-id")
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setAuthors(Lists.newArrayList())
                        .build())
                .setReferences(Lists.newArrayList())
                .build();
        
        assertEquals(expectedDocumentMetadata, retDocumentMetadata);
    }
    
    @Test
    public void convert_WITH_MINIMAL_REFERENCE() {

        // given
        
        eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata inputReference = eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                .setPosition(4)
                .setBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                        .setTitle(null)
                        .setAuthors(null)
                        .setSource(null)
                        .setPages(null)
                        .setYear(null)
                        .build())
                .setText(null)
                .build();
        
        
        ExtractedDocumentMetadataMergedWithOriginal inputDocument = ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId("document-id")
                .setImportedAuthors(Lists.newArrayList())
                .setReferences(Lists.newArrayList(inputReference))
                .setPublicationType(new PublicationType(true, false))
                .build();
        
        
        // execute
        
        DocumentMetadata retDocumentMetadata = converter.convert(inputDocument, emptyReferencePositions);
        
        
        // assert
        
        assertEquals(1, retDocumentMetadata.getReferences().size());
        
        ReferenceMetadata expectedReference = ReferenceMetadata.newBuilder()
                .setPosition(4)
                .setBasicMetadata(BasicMetadata.newBuilder()
                        .setTitle(null)
                        .setAuthors(Lists.newArrayList())
                        .setJournal(null)
                        .setPages(null)
                        .setYear(null)
                        .build())
                .setRawText(null)
                .build();
        
        assertEquals(expectedReference, retDocumentMetadata.getReferences().get(0));
        
    }
    
    // ---------------------------------- PRIVATE --------------------------------------
    
    private Author generateAuthor(String fullName) {
        return Author.newBuilder().setFullname(fullName).build();
    }
    
}
