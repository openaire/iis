package eu.dnetlib.iis.wf.affmatching.write;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * IIS specific implementation of {@link AffMatchResultWriter}
 *  
 * @author Łukasz Dumiszewski
*/

public class IisAffMatchResultWriter implements AffMatchResultWriter {

    private static final long serialVersionUID = 1L;

    
    private AffMatchResultConverter affMatchResultConverter = new AffMatchResultConverter();
    
    private DuplicateMatchedOrgStrengthRecalculator matchedOrgStrengthRecalculation = new DuplicateMatchedOrgStrengthRecalculator();
    
    private SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path as avro objects - {@link MatchedOrganization}s
     */
    @Override
    public void write(JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath) {
        
        Preconditions.checkNotNull(matchedAffOrgs);
        
        Preconditions.checkArgument(StringUtils.isNotBlank(outputPath));


        
        JavaRDD<MatchedOrganization> matchedOrganizations = matchedAffOrgs.map(affOrgMatch -> affMatchResultConverter.convert(affOrgMatch));
        
        
        JavaRDD<MatchedOrganization> distinctMatchedOrganizations = matchedOrganizations
                .keyBy(match -> new Tuple2<>(match.getDocumentId(), match.getOrganizationId()))
                .reduceByKey((match1, match2) -> matchedOrgStrengthRecalculation.recalculateStrength(match1, match2))
                .values();
        
        
        sparkAvroSaver.saveJavaRDD(distinctMatchedOrganizations, MatchedOrganization.SCHEMA$, outputPath);
    }
    
    
}
