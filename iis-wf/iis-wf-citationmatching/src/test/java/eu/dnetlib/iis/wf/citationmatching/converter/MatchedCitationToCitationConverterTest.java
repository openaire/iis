package eu.dnetlib.iis.wf.citationmatching.converter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * @author madryk
 */
public class MatchedCitationToCitationConverterTest {

    private MatchedCitationToCitationConverter converter = new MatchedCitationToCitationConverter();


    //------------------------ TESTS --------------------------

    @Test
    public void convertToCitation() {

        // given

        MatchableEntity citationEntity = MatchableEntity.fromParameters("cit_someId1_4", "John Doe", null, "Some Title", null, null, null);
        IdWithSimilarity docIdWithSimilarity = new IdWithSimilarity("doc_someId2", 0.786);


        // execute

        Citation actualCitation = converter.convertToCitation(citationEntity, docIdWithSimilarity);


        // assert

        Citation expectedCitation = Citation.newBuilder()
                .setSourceDocumentId("someId1")
                .setDestinationDocumentId("someId2")
                .setPosition(4)
                .setConfidenceLevel(0.786F)
                .build();
        assertEquals(expectedCitation, actualCitation);
    }

}
