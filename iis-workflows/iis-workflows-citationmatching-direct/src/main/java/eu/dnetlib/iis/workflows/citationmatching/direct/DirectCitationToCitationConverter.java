package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;

/**
 * Converter of {@link eu.dnetlib.iis.citationmatching.direct.schemas.Citation} object
 * to {@link eu.dnetlib.iis.common.citations.schemas.Citation} object
 * 
 * @author madryk
 *
 */
public class DirectCitationToCitationConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link eu.dnetlib.iis.citationmatching.direct.schemas.Citation}
     * to {@link eu.dnetlib.iis.common.citations.schemas.Citation}
     */
    public Citation convert(eu.dnetlib.iis.citationmatching.direct.schemas.Citation directCitation) {
        
        Preconditions.checkNotNull(directCitation);
        
        
        CitationEntry citationEntry = CitationEntry.newBuilder()
                .setPosition(directCitation.getPosition())
                .setRawText(null)
                .setDestinationDocumentId(directCitation.getDestinationDocumentId())
                .setConfidenceLevel(1f)
                .setExternalDestinationDocumentIds(Maps.newHashMap())
                .build();
        
        Citation citation = Citation.newBuilder()
                .setSourceDocumentId(directCitation.getSourceDocumentId())
                .setEntry(citationEntry)
                .build();
        
        
        return citation;
    }
}
