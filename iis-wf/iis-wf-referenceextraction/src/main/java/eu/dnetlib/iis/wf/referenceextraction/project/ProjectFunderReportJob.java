package eu.dnetlib.iis.wf.referenceextraction.project;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.io.IOException;

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
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            JavaRDD<DocumentToProject> documentToProject = avroLoader.loadJavaRDD(sc, params.inputDocumentToProjectAvroPath, DocumentToProject.class);
            documentToProject.cache();
            
            ReportEntry totalReportEntry = new ReportEntry(params.reportKeyTotal, ReportEntryType.COUNTER, 
                    String.valueOf(documentToProject.count()));
            
            JavaRDD<Project> project = avroLoader.loadJavaRDD(sc, params.inputProjectAvroPath, Project.class);

            JavaPairRDD<CharSequence, Integer> projIdToOne = documentToProject
                    .mapToPair(x -> new Tuple2<>(x.getProjectId(), 1));
            JavaPairRDD<CharSequence, String> projIdToFunder = project
                    .mapToPair(x -> new Tuple2<>(x.getId(), extractFunderName(x.getFundingClass())));
            JavaPairRDD<CharSequence, Tuple2<Integer, String>> joinedByProjectId = projIdToOne
                    .join(projIdToFunder);
            JavaPairRDD<CharSequence, Integer> funderWithOne = joinedByProjectId
                    .mapToPair(x -> new Tuple2<>(x._2._2, x._2._1));
            JavaPairRDD<CharSequence, Integer> reducedFunderWithCount = funderWithOne
                    .reduceByKey(Integer::sum);

            JavaRDD<ReportEntry> funderReport = convertToReportEntries(reducedFunderWithCount.sortByKey(true), params.reportKeyTemplate);

            avroSaver.saveJavaRDD(funderReport.union(sc.parallelize(Lists.newArrayList(totalReportEntry))).repartition(1),
                    ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }
    
    //------------------------ PRIVATE --------------------------

    /**
     * Extracts funder name out of the funding class.
     * Never returns null, "unknown" value is returned when funder detail is not available. 
     */
    private static String extractFunderName(CharSequence fundingClass) {
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
    private static JavaRDD<ReportEntry> convertToReportEntries(JavaPairRDD<CharSequence, Integer> source,
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
        
        @Parameter(names = "-reportKeyTotal", required = true)
        private String reportKeyTotal;
    }
}
