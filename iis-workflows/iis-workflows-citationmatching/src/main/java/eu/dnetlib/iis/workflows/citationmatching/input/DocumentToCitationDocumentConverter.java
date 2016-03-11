package eu.dnetlib.iis.workflows.citationmatching.input;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
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
     * Notice that returned document metadata will contain author ids list in {@link BasicMetadata#getAuthors()}
     * instead of author names.
     */
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal sourceDocument) {
        
        Preconditions.checkNotNull(sourceDocument);
        
        DocumentMetadata destDocument = DocumentMetadata.newBuilder()
                .setId(sourceDocument.getId())
                .setBasicMetadata(convertBasicMetadata(sourceDocument))
                .setReferences(convertReferences(sourceDocument.getReferences()))
                .build();
        
        return destDocument;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private BasicMetadata convertBasicMetadata(ExtractedDocumentMetadataMergedWithOriginal sourceDocument) {
        return BasicMetadata.newBuilder()
                .setAuthors((sourceDocument.getAuthorIds() == null) ? Lists.newArrayList() : sourceDocument.getAuthorIds())
                .setJournal(sourceDocument.getJournal())
                .setPages(convertRange(sourceDocument.getPages()))
                .setTitle(sourceDocument.getTitle())
                .setYear((sourceDocument.getYear() == null) ? null : sourceDocument.getYear().toString())
                .build();
    }
    
    private List<ReferenceMetadata> convertReferences(List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> sourceReferences) {
        
        List<ReferenceMetadata> destReferences = Lists.newArrayList();
        
        if (sourceReferences == null) {
            return destReferences;
        }
        
        sourceReferences.forEach(sourceReference -> destReferences.add(convertReference(sourceReference)));
        
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
