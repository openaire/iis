package eu.dnetlib.iis.wf.affmatching.write;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.schemas.ReportEntry;
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
    
    private AffMatchReportGenerator reportGenerator = new AffMatchReportGenerator();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path as avro objects - {@link MatchedOrganization}s
     */
    @Override
    public void write(JavaSparkContext sc, JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath, String outputReportPath) {
        
        Preconditions.checkNotNull(sc);
        
        Preconditions.checkNotNull(matchedAffOrgs);
        
        Preconditions.checkArgument(StringUtils.isNotBlank(outputPath));

        Preconditions.checkArgument(StringUtils.isNotBlank(outputReportPath));

        
        JavaRDD<MatchedOrganization> matchedOrganizations = matchedAffOrgs.map(affOrgMatch -> affMatchResultConverter.convert(affOrgMatch));
        
        
        JavaRDD<MatchedOrganization> distinctMatchedOrganizations = matchedOrganizations
                .keyBy(match -> new Tuple2<>(match.getDocumentId(), match.getOrganizationId()))
                .reduceByKey((match1, match2) -> matchedOrgStrengthRecalculation.recalculateStrength(match1, match2))
                .values();
        
        distinctMatchedOrganizations.cache();
        
        sparkAvroSaver.saveJavaRDD(distinctMatchedOrganizations, MatchedOrganization.SCHEMA$, outputPath);
        
        
        
        List<ReportEntry> reportEntries = reportGenerator.generateReport(distinctMatchedOrganizations);
        
        sparkAvroSaver.saveJavaRDD(sc.parallelize(reportEntries), ReportEntry.SCHEMA$, outputReportPath);
        

        
    }
    
    
    
}
