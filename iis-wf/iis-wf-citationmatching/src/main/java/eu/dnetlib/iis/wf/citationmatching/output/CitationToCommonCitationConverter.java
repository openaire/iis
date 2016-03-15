package eu.dnetlib.iis.wf.citationmatching.output;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;

/**
 * Converter of {@link eu.dnetlib.iis.citationmatching.schemas.Citation} 
 * to {@link eu.dnetlib.iis.common.citations.schemas.Citation}
 * 
 * @author madryk
 */
public class CitationToCommonCitationConverter {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link eu.dnetlib.iis.citationmatching.schemas.Citation}
     * to {@link eu.dnetlib.iis.common.citations.schemas.Citation}
     */
    public eu.dnetlib.iis.common.citations.schemas.Citation convert(Citation inputCitation) {
        
        Preconditions.checkNotNull(inputCitation);
        
        
        CitationEntry citationEntry = CitationEntry.newBuilder()
                .setPosition(inputCitation.getPosition())
                .setDestinationDocumentId(inputCitation.getDestinationDocumentId())
                .setConfidenceLevel(inputCitation.getConfidenceLevel())
                .setExternalDestinationDocumentIds(Maps.newHashMap())
                .build();
        
        return eu.dnetlib.iis.common.citations.schemas.Citation.newBuilder()
                .setSourceDocumentId(inputCitation.getSourceDocumentId())
                .setEntry(citationEntry)
                .build();
    }
}
