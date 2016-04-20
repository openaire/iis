package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * Implementation of {@link AffiliationReader} that reads IIS affiliations, {@link Affiliation} objects written
 * in avro files. 

 * @author Łukasz Dumiszewski
*/

public class IisAffiliationReader implements Serializable, AffiliationReader {

    private static final long serialVersionUID = 1L;
    
    
    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    private AffiliationConverter affiliationConverter = new AffiliationConverter();
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads {@link Affiliation}s written as avro files under <code>inputPath</code>
     */
    @Override
    public JavaRDD<AffMatchAffiliation> readAffiliations(JavaSparkContext sc, String inputPath) {
        
        JavaRDD<ExtractedDocumentMetadata> sourceAffiliations = avroLoader.loadJavaRDD(sc, inputPath, ExtractedDocumentMetadata.class);
        JavaRDD<AffMatchAffiliation> affiliations = sourceAffiliations.flatMap(srcAff -> affiliationConverter.convert(srcAff));  
        
        return affiliations;
        
    }
    
    
}
