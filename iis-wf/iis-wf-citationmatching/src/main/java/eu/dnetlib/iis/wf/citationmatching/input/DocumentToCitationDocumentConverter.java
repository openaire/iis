package eu.dnetlib.iis.wf.citationmatching.input;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * Converter of {@link ExtractedDocumentMetadataMergedWithOriginal} object to
 * {@link DocumentMetadata}
 * 
 * @author madryk
 */
public class DocumentToCitationDocumentConverter {


    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link ExtractedDocumentMetadataMergedWithOriginal} to {@link DocumentMetadata}.<br/>
     * @param sourceDocument source document to be converted
     * @param matchedReferencePositions already matched positions of references which should be removed from input, cannot be null
     * @param discardAllReferences flag indicating all references should be discarded
     * 
     */
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal sourceDocument, Set<Integer> matchedReferencePositions, boolean discardAllReferences) {
        
        Preconditions.checkNotNull(sourceDocument);
        
        DocumentMetadata destDocument = DocumentMetadata.newBuilder()
                .setId(sourceDocument.getId())
                .setBasicMetadata(convertBasicMetadata(sourceDocument))
                .setReferences(convertReferences(sourceDocument.getReferences(), matchedReferencePositions, discardAllReferences))
                .build();
        
        return destDocument;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private BasicMetadata convertBasicMetadata(ExtractedDocumentMetadataMergedWithOriginal sourceDocument) {
        return BasicMetadata.newBuilder()
                .setAuthors(extractAuthorsFullNames(sourceDocument.getImportedAuthors()))
                .setJournal(sourceDocument.getJournal())
                .setPages(convertRange(sourceDocument.getPages()))
                .setTitle(sourceDocument.getTitle())
                .setYear((sourceDocument.getYear() == null) ? null : sourceDocument.getYear().toString())
                .build();
    }
    
    private List<CharSequence> extractAuthorsFullNames(List<Author> authors) {
        return authors != null
                ? authors.stream().filter(p -> p.getFullname() != null).map(p -> p.getFullname()).collect(Collectors.toList())
                : Lists.newArrayList();
    }
    
    private List<ReferenceMetadata> convertReferences(List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> sourceReferences,
            Set<Integer> matchedReferencePositions, boolean discardAllReferences) {
        
        List<ReferenceMetadata> destReferences = Lists.newArrayList();
        
        if (sourceReferences == null || discardAllReferences) {
            return destReferences;
        }
        
        sourceReferences.stream()
                .filter(sourceReference -> !matchedReferencePositions.contains(sourceReference.getPosition()))
                .forEach(sourceReference -> destReferences.add(convertReference(sourceReference)));
        
        return destReferences;
    }
    
    private ReferenceMetadata convertReference(eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata sourceRefMeta) {
        
        ReferenceMetadata refMeta = ReferenceMetadata.newBuilder()
                .setBasicMetadata(convertReferenceBasicMetadata(sourceRefMeta.getBasicMetadata()))
                .setPosition(sourceRefMeta.getPosition())
                .setRawText(sourceRefMeta.getText())
                .build();
        
        return refMeta;
        
    }
    
    private BasicMetadata convertReferenceBasicMetadata(eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata sourceRefBasicMeta) {
        return BasicMetadata.newBuilder()
                .setAuthors((sourceRefBasicMeta.getAuthors() == null) ? Lists.newArrayList() : sourceRefBasicMeta.getAuthors())
                .setJournal(sourceRefBasicMeta.getSource())
                .setPages(convertRange(sourceRefBasicMeta.getPages()))
                .setTitle(sourceRefBasicMeta.getTitle())
                .setYear(sourceRefBasicMeta.getYear())
                .build();
    }
    
    private String convertRange(eu.dnetlib.iis.metadataextraction.schemas.Range range) {
        return (range == null) ? null : range.getStart() + "-" + range.getEnd();
    }
}
