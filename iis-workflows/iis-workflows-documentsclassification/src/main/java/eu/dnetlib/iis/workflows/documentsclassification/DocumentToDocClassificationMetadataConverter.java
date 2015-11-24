package eu.dnetlib.iis.workflows.documentsclassification;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * {@link ExtractedDocumentMetadataMergedWithOriginal} to {@link DocumentMetadata} converter
 * @author Łukasz Dumiszewski
 */

public class DocumentToDocClassificationMetadataConverter {

    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Converts the given {@link ExtractedDocumentMetadataMergedWithOriginal} to {@link DocumentMetadata}
     */
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal document) {
        
        Preconditions.checkNotNull(document);
        
        DocumentMetadata metadata = new DocumentMetadata();
        
        metadata.setAbstract$(document.getAbstract$());
        
        metadata.setId(document.getId());
        
        return metadata;
    }
    
    
}
