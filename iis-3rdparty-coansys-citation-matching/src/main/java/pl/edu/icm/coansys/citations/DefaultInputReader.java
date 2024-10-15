package pl.edu.icm.coansys.citations;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Default input reader of documents and citations.
 * If no input readers are specified for citation matching job
 * then this reader is used.
 * Reader assumes that input data is a sequence file where
 * keys are entity identifiers (saved as {@link Text}) and values
 * are {@link MatchableEntity}s (saved as {@link BytesWritable})
 * 
 * @author madryk
 */
public class DefaultInputReader implements InputCitationReader<String, MatchableEntity>, InputDocumentReader<String, MatchableEntity> {
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns spark rdd containing documents.
     * Keys of returned rdd are strings containing entity id.
     * Values of returned rdd are {@link MatchableEntity} objects
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> readDocuments(JavaSparkContext sparkContext, String inputDocumentPath) {
        return readEntities(sparkContext, inputDocumentPath);
    }

    /**
     * Returns spark rdd containing citations.
     * Keys of returned rdd are strings containing entity id.
     * Values of returned rdd are {@link MatchableEntity} objects
     */
    @Override
    public JavaPairRDD<String, MatchableEntity> readCitations(JavaSparkContext sparkContext, String inputCitationPath) {
        return readEntities(sparkContext, inputCitationPath);
    }

    
    //------------------------ PRIVATE --------------------------
    
    private JavaPairRDD<String, MatchableEntity> readEntities(JavaSparkContext sparkContext, String entitesPath) {
        
        JavaPairRDD<Text, BytesWritable> readEntities = sparkContext.sequenceFile(entitesPath, Text.class, BytesWritable.class);
        
        JavaPairRDD<String, MatchableEntity> entities = readEntities.mapToPair(x -> new Tuple2<String, MatchableEntity>(x._1.toString(), MatchableEntity.fromBytes(x._2.copyBytes())));
        
        
        return entities;
    }
    

}
