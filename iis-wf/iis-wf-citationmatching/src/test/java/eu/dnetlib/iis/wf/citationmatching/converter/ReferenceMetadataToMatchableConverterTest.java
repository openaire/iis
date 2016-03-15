package eu.dnetlib.iis.wf.citationmatching.converter;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * @author madryk
 */
public class ReferenceMetadataToMatchableConverterTest {

    private ReferenceMetadataToMatchableConverter converter = new ReferenceMetadataToMatchableConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void convertToMatchableEntity() {
        
        // given
        
        BasicMetadata basicMetadata = BasicMetadata.newBuilder()
                .setAuthors(Lists.newArrayList("John Doe"))
                .setJournal("Some Journal")
                .setTitle("Some Title")
                .setPages("55-64")
                .setYear("2002")
                .build();
        ReferenceMetadata referenceMetadata = ReferenceMetadata.newBuilder()
                .setPosition(5)
                .setBasicMetadata(basicMetadata)
                .setRawText("rawText")
                .build();
        
        
        // execute
        
        MatchableEntity actualMatchableEntity = converter.convertToMatchableEntity("cit_someId", referenceMetadata);
        
        
        // assert
        
        MatchableEntity expectedMatchableEntity = MatchableEntity.fromParameters("cit_someId",
                "John Doe", "Some Journal", "Some Title", "55-64", "2002", "rawText");
        assertTrue(Arrays.equals(actualMatchableEntity.data().toByteArray(), expectedMatchableEntity.data().toByteArray()));
    }
    
    
    @Test
    public void convertToMatchableEntity_MULTIPLE_AUTHORS() {
        
        // given
        
        BasicMetadata basicMetadata = BasicMetadata.newBuilder()
                .setAuthors(Lists.newArrayList("John Doe", "Jane Doe"))
                .build();
        ReferenceMetadata referenceMetadata = ReferenceMetadata.newBuilder()
                .setPosition(5)
                .setBasicMetadata(basicMetadata)
                .setRawText("rawText")
                .build();
        
        
        // execute
        
        MatchableEntity actualMatchableEntity = converter.convertToMatchableEntity("cit_someId", referenceMetadata);
        
        
        // assert
        
        MatchableEntity expectedMatchableEntity = MatchableEntity.fromParameters("cit_someId",
                "John Doe, Jane Doe", null, null, null, null, "rawText");
        assertTrue(Arrays.equals(actualMatchableEntity.data().toByteArray(), expectedMatchableEntity.data().toByteArray()));
    }
    
    
    @Test
    public void convertToMatchableEntity_NULL_RAW_TEXT() {
        
        // given
        
        BasicMetadata basicMetadata = BasicMetadata.newBuilder()
                .setAuthors(Lists.newArrayList("John Doe"))
                .setJournal("Some Journal")
                .setTitle("Some Title")
                .setPages("55-64")
                .setYear("2002")
                .build();
        ReferenceMetadata referenceMetadata = ReferenceMetadata.newBuilder()
                .setPosition(5)
                .setBasicMetadata(basicMetadata)
                .setRawText(null)
                .build();
        
        
        // execute
        
        MatchableEntity actualMatchableEntity = converter.convertToMatchableEntity("cit_someId", referenceMetadata);
        
        
        // assert
        
        MatchableEntity expectedMatchableEntity = MatchableEntity.fromParameters("cit_someId",
                "John Doe", "Some Journal", "Some Title", "55-64", "2002", "John Doe: Some Title. Some Journal (2002) 55-64");
        assertTrue(Arrays.equals(actualMatchableEntity.data().toByteArray(), expectedMatchableEntity.data().toByteArray()));
    }
}
