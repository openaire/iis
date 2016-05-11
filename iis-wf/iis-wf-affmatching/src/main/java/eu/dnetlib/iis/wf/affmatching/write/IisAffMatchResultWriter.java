package eu.dnetlib.iis.wf.affmatching.write;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

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
    
    private BestMatchedAffiliationWithinDocumentPicker bestMatchedAffiliationWithinDocumentPicker = new BestMatchedAffiliationWithinDocumentPicker();
    
    private SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path as avro objects - {@link MatchedAffiliation}s
     */
    @Override
    public void write(JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath) {
        
        Preconditions.checkNotNull(matchedAffOrgs);
        
        Preconditions.checkArgument(StringUtils.isNotBlank(outputPath));


        JavaRDD<MatchedAffiliation> matchedAffiliations = matchedAffOrgs.map(affOrgMatch -> affMatchResultConverter.convert(affOrgMatch));
        
        JavaRDD<MatchedAffiliation> documentUniqueMatchedAffiliations = matchedAffiliations
                .keyBy(match -> match.getDocumentId())
                .groupByKey()
                .mapValues(matches -> bestMatchedAffiliationWithinDocumentPicker.pickBest(matches))
                .values();
        
        
        sparkAvroSaver.saveJavaRDD(documentUniqueMatchedAffiliations, MatchedAffiliation.SCHEMA$, outputPath);
    }
    
    
}
