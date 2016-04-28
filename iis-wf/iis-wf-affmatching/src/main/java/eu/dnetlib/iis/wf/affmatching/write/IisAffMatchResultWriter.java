package eu.dnetlib.iis.wf.affmatching.write;

import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * IIS specific implementation of {@link AffMatchResultWriter}
 *  
 * @author Łukasz Dumiszewski
*/

public class IisAffMatchResultWriter implements AffMatchResultWriter {

    private static final long serialVersionUID = 1L;

    
    private AffMatchResultConverter affMatchResultConverter = new AffMatchResultConverter();
    
    private SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path as avro objects - {@link MatchedAffiliation}s
     */
    @Override
    public void write(JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath) {
        
        JavaRDD<MatchedAffiliation> matchedAffiliations = matchedAffOrgs.map(affOrgMatch -> affMatchResultConverter.convert(affOrgMatch));
        
        sparkAvroSaver.saveJavaRDD(matchedAffiliations, MatchedAffiliation.SCHEMA$, outputPath);
    }
    
}
