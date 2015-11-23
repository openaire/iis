package eu.dnetlib.iis.workflows.documentsclassification;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentToDocClassificationMetadataConverter {

    
    
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal document) {
        
        Preconditions.checkNotNull(document);
        
        DocumentMetadata metadata = new DocumentMetadata();
        
        metadata.setAbstract$(document.getAbstract$());
        
        metadata.setId(document.getId());
        
        return metadata;
    }
    
    
}
