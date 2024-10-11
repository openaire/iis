package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Dummy converter of input documents and citations.
 * It has empty implementation of converting citations and documents.
 * 
 * @author madryk
 *
 */
public class DummyInputConverter implements InputDocumentConverter<String, MatchableEntity>, InputCitationConverter<String, MatchableEntity> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns the same citations rdd that was specified as argument.
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> convertCitations(JavaPairRDD<String, MatchableEntity> inputCitations) {
        return inputCitations;
    }

    /**
     * Returns the same documents rdd that was specified as argument.
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> convertDocuments(JavaPairRDD<String, MatchableEntity> inputDocuments) {
        return inputDocuments;
    }

}
