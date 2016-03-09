package eu.dnetlib.iis.wf.collapsers.origins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEnvelope;

/**
 * Collapses {@link Citation} records coming from pmc and cermine origins
 * by merging referenced document identifiers. 
 * 
 * @author mhorst
 */
public class PMCCitationCollapser extends OriginCollapser<CitationEnvelope, Citation> {

	private final String ORIGIN_INGESTED = "ingested";
	private final String ORIGIN_MATCHED = "matched";
	
    @Override
    protected List<Citation> collapseBetweenOrigins(Map<String, List<Citation>> objects) {
        if (objects.size()==1) {
        	return objects.values().iterator().next();
        } else if (objects.size()==2) {
//        	important note: identifying cermine matches by referenceText
        	Map<String,Citation> cermineCitationsMap = new HashMap<String, Citation>();
        	for (Citation currentCermineCitation : objects.get(ORIGIN_MATCHED)) {
        		if (currentCermineCitation.getEntry()!=null && 
        				currentCermineCitation.getEntry().getRawText()!=null) {
        			cermineCitationsMap.put(
            				currentCermineCitation.getEntry().getRawText().toString(), 
            				currentCermineCitation);	
        		}
        	}
        	List<Citation> results = new ArrayList<Citation>();
        	for (Citation currentPmcCitation : objects.get(ORIGIN_INGESTED)) {
        		if (currentPmcCitation.getEntry()!=null && 
        				currentPmcCitation.getEntry().getRawText()!=null) {
        			Citation merged = merge(currentPmcCitation, 
        					cermineCitationsMap.remove(
                					currentPmcCitation.getEntry().getRawText().toString()));
        			if (merged!=null) {
        				results.add(merged);	
        			}
        		}
        	}
//        	returning all the cermine remainings, should not happen though
        	if (!cermineCitationsMap.isEmpty()) {
        		results.addAll(cermineCitationsMap.values());	
        	}
        	return results;
        } else {
//        	we need to be strict to conduct collapsing process properly
        	throw new RuntimeException("only two origins are supported: " + 
        			ORIGIN_INGESTED + " and " + ORIGIN_MATCHED + 
        			" got: " + objects.keySet());
        }
    }
    
    private static final Citation merge(Citation pmcCitation, Citation cermineCitation) {
    	if (pmcCitation!=null) {
    		if (cermineCitation!=null) {
    			if (pmcCitation.getEntry().getDestinationDocumentId()==null &&
    					cermineCitation.getEntry().getDestinationDocumentId()!=null) {
    				pmcCitation.getEntry().setDestinationDocumentId(
    						cermineCitation.getEntry().getDestinationDocumentId());
    				pmcCitation.getEntry().setConfidenceLevel(
    						cermineCitation.getEntry().getConfidenceLevel());
    			}
    		}
    		return pmcCitation;
    	} else {
    		return cermineCitation;
    	}
    }

}
 