package eu.dnetlib.iis.wf.citationmatching;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.wf.citationmatching.converter.entity_id.CitEntityId;
import pl.edu.icm.coansys.citations.InputCitationReader;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * Reader of input citations rdd
 * 
 * @author madryk
 */
public class ReferenceMetadataInputReader implements InputCitationReader<String, ReferenceMetadata>, Serializable {

    private static final long serialVersionUID = 1L;


    private final SparkAvroLoader avroLoader = new SparkAvroLoader();

    //------------------------ LOGIC --------------------------

    /**
     * Reads input citations rdd from avro {@link DocumentMetadata} datastore.
     * Keys of returned rdd will contain citation id.
     * Id of citation is built by adding {@literal cit_} prefix
     * and {@literal _position} to document id.
     * Values of returned rdd will contain citation in form of {@link ReferenceMetadata} object.
     */
    @Override
    public JavaPairRDD<String, ReferenceMetadata> readCitations(JavaSparkContext sparkContext, String inputCitationsPath) {

        JavaRDD<DocumentMetadata> fullDocuments = avroLoader.loadJavaRDD(sparkContext, inputCitationsPath, DocumentMetadata.class);

        JavaPairRDD<String, ReferenceMetadata> references = fullDocuments.flatMapToPair(fullDocument -> {
            return fullDocument.getReferences().stream().map(
                    reference -> new Tuple2<>(buildCitationId(fullDocument.getId().toString(), reference), reference))
                    .iterator();
        });

        return references;
    }


    //------------------------ PRIVATE --------------------------

    private String buildCitationId(String documentId, ReferenceMetadata referenceMetadata) {
        return new CitEntityId(documentId, referenceMetadata.getPosition()).toString();
    }

}
