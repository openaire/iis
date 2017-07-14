package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * Dataset entities filter.
 * 
 * @author mhorst
 *
 */
public class DatasetFilter implements EntityFilter {
    
    
    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    //------------------------ LOGIC --------------------------

    @Override
    public JavaRDD<CharSequence> provideRDD(JavaSparkContext sc, String relationsPath, String entitiesPath) {
        
        JavaRDD<DocumentToDataSet> documentToDataset = avroLoader.loadJavaRDD(sc, relationsPath, DocumentToDataSet.class);
        
        JavaRDD<DocumentText> datasetText = avroLoader.loadJavaRDD(sc, entitiesPath, DocumentText.class);

        JavaPairRDD<CharSequence, Object> dedupedDatasetIdToBlank = documentToDataset.mapToPair(x -> new Tuple2<>(x.getDatasetId(), null)).distinct();

        JavaPairRDD<CharSequence, CharSequence> datasetIdToText = datasetText.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
        
        JavaPairRDD<CharSequence,Tuple2<Object,CharSequence>> joinedById = dedupedDatasetIdToBlank.join(datasetIdToText);
        
        return joinedById.map(x -> x._2._2);
    }

}
