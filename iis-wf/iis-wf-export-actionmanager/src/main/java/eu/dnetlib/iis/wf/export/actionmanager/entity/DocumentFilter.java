package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * Document entities filter.
 * 
 * @author mhorst
 *
 */
public class DocumentFilter implements EntityFilter {
    
    
    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    //------------------------ LOGIC --------------------------

    @Override
    public JavaRDD<CharSequence> provideRDD(JavaSparkContext sc, String relationsPath, String entitiesPath) {
        
        JavaRDD<DocumentToProject> documentToProject = avroLoader.loadJavaRDD(sc, relationsPath, DocumentToProject.class);
        
        JavaRDD<DocumentText> documentText = avroLoader.loadJavaRDD(sc, entitiesPath, DocumentText.class);

        JavaPairRDD<CharSequence, Object> dedupedDocumentIdToBlank = documentToProject.mapToPair(x -> new Tuple2<>(x.getDocumentId(), null)).distinct();

        JavaPairRDD<CharSequence, CharSequence> documentIdToText = documentText.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
        
        JavaPairRDD<CharSequence,Tuple2<Object,CharSequence>> joinedById = dedupedDocumentIdToBlank.join(documentIdToText);
        
        return joinedById.map(x -> x._2._2);
    }

}
