package eu.dnetlib.iis.wf.referenceextraction.project;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;


/**
 * Generates {@link DocumentToProject} relation counters grouped by funders.
 * @author mhorst
 *
 */
public class ProjectFunderReportJob {
    
    private static final String FUNDER_FUNDING_SEPARATOR = "::";
    
    private static final String FUNDER_TOKEN = "#{funder}";
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        ProjectFunderReportJobParameters params = new ProjectFunderReportJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            JavaRDD<DocumentToProject> documentToProject = avroLoader.loadJavaRDD(sc, params.inputDocumentToProjectAvroPath, DocumentToProject.class);
            JavaRDD<Project> project = avroLoader.loadJavaRDD(sc, params.inputProjectAvroPath, Project.class);

            JavaPairRDD<CharSequence, Integer> projIdToOne = documentToProject.mapToPair(x -> new Tuple2<CharSequence, Integer>(x.getProjectId(), 1));
            JavaPairRDD<CharSequence, String> projIdToFunder = project.mapToPair(x -> new Tuple2<CharSequence, String>(
                    x.getId(), extractFunderName(x.getFundingClass())));
            JavaPairRDD<CharSequence, Tuple2<Integer, String>> joinedByProjectId = projIdToOne.join(projIdToFunder);
            JavaPairRDD<CharSequence, Integer> funderWithOne = joinedByProjectId.mapToPair(x -> new Tuple2<CharSequence, Integer>(x._2._2, x._2._1));
            JavaPairRDD<CharSequence, Integer> reducedFunderWithCount = funderWithOne.reduceByKey((x, y) -> x+y);

            avroSaver.saveJavaRDD(convertToReportEntries(reducedFunderWithCount.sortByKey(true), params.reportKeyTemplate), 
                    ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }
    
    //------------------------ PRIVATE --------------------------

    /**
     * Extracts funder name out of the funding class.
     * Never returns null, "unknown" value is returned when funder detail is not available. 
     */
    static String extractFunderName(CharSequence fundingClass) {
        if (fundingClass != null) {
            String fundingClassStr = fundingClass.toString();
            if (fundingClassStr.contains(FUNDER_FUNDING_SEPARATOR) && !fundingClassStr.startsWith(FUNDER_FUNDING_SEPARATOR)) {
                String funder = StringUtils.splitByWholeSeparator(fundingClassStr, FUNDER_FUNDING_SEPARATOR)[0];
                if (!StringUtils.isBlank(funder)) {
                    return funder;
                }
            }
        }
        return "unknown";
    }
    
    /**
     * Converts all funder names into report entry keys using template and replacing FUNDER_TOKEN with real funder name.
     */
    static JavaRDD<ReportEntry> convertToReportEntries(JavaPairRDD<CharSequence, Integer> source,
            String reportKeyTemplate) {
        return source.map(x -> new ReportEntry(
                StringUtils.replace(reportKeyTemplate, FUNDER_TOKEN, x._1.toString().toLowerCase()), 
                ReportEntryType.COUNTER, String.valueOf(x._2)));
    }
    
    @Parameters(separators = "=")
    private static class ProjectFunderReportJobParameters {
        
        @Parameter(names = "-inputProjectAvroPath", required = true)
        private String inputProjectAvroPath;
        
        @Parameter(names = "-inputDocumentToProjectAvroPath", required = true)
        private String inputDocumentToProjectAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-reportKeyTemplate", required = true)
        private String reportKeyTemplate;
    }
}
