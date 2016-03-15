package eu.dnetlib.iis.wf.citationmatching.converter;

import java.io.Serializable;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.wf.citationmatching.converter.entity_id.CitEntityId;
import eu.dnetlib.iis.wf.citationmatching.converter.entity_id.DocEntityId;
import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of {@link MatchableEntity} and {@link IdWithSimilarity} objects
 * to {@link Citation} object
 * 
 * @author madryk
 */
public class MatchedCitationToCitationConverter implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    /**
     * Returns {@link Citation} converted from {@link MatchableEntity} and {@link IdWithSimilarity} objects
     */
    public Citation convertToCitation(MatchableEntity citationEntity, IdWithSimilarity docIdWithSimilarity) {

        CitEntityId citEntityId = CitEntityId.parseFrom(citationEntity.id());
        DocEntityId docEntityId = DocEntityId.parseFrom(docIdWithSimilarity.getId());

        Citation cit = new Citation();

        cit.setSourceDocumentId(citEntityId.getSourceDocumentId());
        cit.setPosition(citEntityId.getPosition());
        cit.setDestinationDocumentId(docEntityId.getDocumentId());
        cit.setConfidenceLevel((float)docIdWithSimilarity.getSimilarity());

        return cit;
    }

}
