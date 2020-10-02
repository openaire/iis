package eu.dnetlib.iis.wf.citationmatching.converter;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import org.junit.jupiter.api.Test;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * @author madryk
 */
public class DocumentMetadataToMatchableConverterTest {

    private DocumentMetadataToMatchableConverter converter = new DocumentMetadataToMatchableConverter();


    //------------------------ TESTS --------------------------

    @Test
    public void convertToMatchableEntity() {

        // given

        BasicMetadata basicDocMetadata = BasicMetadata.newBuilder()
                .setAuthors(Lists.newArrayList("John Doe"))
                .setJournal("Some Journal")
                .setTitle("Some Title")
                .setPages("55-64")
                .setYear("2002")
                .build();
        DocumentMetadata documentMetadata = DocumentMetadata.newBuilder()
                .setId("doc_someId")
                .setBasicMetadata(basicDocMetadata)
                .setReferences(Lists.newArrayList())
                .build();


        // execute

        MatchableEntity actualMatchableEntity = converter.convertToMatchableEntity("doc_someId", documentMetadata);


        // assert

        MatchableEntity expectedMatchableEntity = MatchableEntity.fromParameters("doc_someId", "John Doe", "Some Journal", "Some Title", "55-64", "2002", null);
        assertArrayEquals(actualMatchableEntity.data().toByteArray(), expectedMatchableEntity.data().toByteArray());
    }


    @Test
    public void convertToMatchableEntity_MULTIPLE_AUTHORS() {

        // given

        BasicMetadata basicDocMetadata = BasicMetadata.newBuilder()
                .setAuthors(Lists.newArrayList("John Doe", "Jane Doe"))
                .build();
        DocumentMetadata documentMetadata = DocumentMetadata.newBuilder()
                .setId("doc_someId")
                .setBasicMetadata(basicDocMetadata)
                .setReferences(Lists.newArrayList())
                .build();


        // execute

        MatchableEntity actualMatchableEntity = converter.convertToMatchableEntity("doc_someId", documentMetadata);


        // assert

        MatchableEntity expectedMatchableEntity = MatchableEntity.fromParameters("doc_someId", "John Doe, Jane Doe", null, null, null, null, null);
        assertArrayEquals(actualMatchableEntity.data().toByteArray(), expectedMatchableEntity.data().toByteArray());
    }

}
