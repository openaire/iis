package eu.dnetlib.iis.wf.affmatching;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.*;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.write.ProjectBasedDocOrgMatchReportGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Job matching publications with organizations based on publication->project and project->organization relations.
 * Only projects with single organization will be taken into account. 
 * 
 * 
 * @author mhorst
 */
public class ProjectBasedMatchingJob {

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
    	ProjectRelatedDocOrgMatchingJobParameters params = new ProjectRelatedDocOrgMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
    
        SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
        ProjectBasedDocOrgMatchReportGenerator reportGenerator = new ProjectBasedDocOrgMatchReportGenerator();
        
        final String inputAvroDocProjPath = params.inputAvroDocProjPath;
        final String inputAvroInferredDocProjPath = params.inputAvroInferredDocProjPath;
        final String inputAvroProjOrgPath = params.inputAvroProjOrgPath;
        final String inputAvroProjectPath = params.inputAvroProjectPath;
        
        final String projectFundingClassWhitelistRegex = params.projectFundingClassWhitelistRegex;
        final Float docProjConfidenceThreshold = params.inputDocProjConfidenceThreshold;
        
        final String outputPath = params.outputAvroPath;
        final String outputReportPath = params.outputAvroReportPath;
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroReportPath);

            DocumentProjectFetcher documentProjectFetcher = new DocumentProjectFetcher();
            documentProjectFetcher.setFirstDocumentProjectReader(new IisDocumentProjectReader());
            documentProjectFetcher.setSecondDocumentProjectReader(new IisInferredDocumentProjectReader());
            documentProjectFetcher.setDocumentProjectMerger(new DocumentProjectMerger());
            documentProjectFetcher.setSparkContext(sc);
            documentProjectFetcher.setFirstDocProjPath(inputAvroDocProjPath);
            documentProjectFetcher.setSecondDocProjPath(inputAvroInferredDocProjPath);
            
            IisProjectOrganizationReader projectOrganizationReader = new IisProjectOrganizationReader();
            
            JavaRDD<AffMatchDocumentProject> docProj = documentProjectFetcher.fetchDocumentProjects();
            
            JavaRDD<AffMatchDocumentProject> filteredDocProj = docProjConfidenceThreshold != null
                    ? docProj.filter(e -> e.getConfidenceLevel() >= docProjConfidenceThreshold)
                    : docProj;
            
            JavaRDD<AffMatchProjectOrganization> projOrg = projectOrganizationReader.readProjectOrganizations(sc, inputAvroProjOrgPath);
            
            JavaPairRDD<String, CharSequence> projectToFundingClassFiltered =
                    getProjectToFundingClass(sc, inputAvroProjectPath, projectFundingClassWhitelistRegex);
            
            // accepting project relations with accepted funder and single organization only
            JavaRDD<AffMatchProjectOrganization> filteredProjOrg = projOrg
                    .keyBy(AffMatchProjectOrganization::getProjectId)
                    .join(projectToFundingClassFiltered)
                    .mapToPair(e -> new Tuple2<>(e._1, e._2._1))
                    .groupByKey()
                    .flatMap(e -> getRelationsWithSingleOrganization(e._2).iterator());
            
            JavaRDD<MatchedOrganization> matchedDocOrg = filteredDocProj
                    .keyBy(AffMatchDocumentProject::getProjectId)
                    .join(filteredProjOrg.keyBy(AffMatchProjectOrganization::getProjectId))
                    .map(ProjectBasedMatchingJob::buildMatchedOrganization);
            
            // deduplicating by grouping by doc+org and picking the highest confidence level
            JavaRDD<MatchedOrganization> distinctMatchedOrganizations = matchedDocOrg
                    .keyBy(ProjectBasedMatchingJob::buildMatchedOrgKey)
                    .reduceByKey((x, y) -> x.getMatchStrength() >= y.getMatchStrength() ? x : y)
                    .values();
            
            distinctMatchedOrganizations.cache();
            
            sparkAvroSaver.saveJavaRDD(distinctMatchedOrganizations, MatchedOrganization.SCHEMA$, outputPath);
            
            List<ReportEntry> reportEntries = reportGenerator.generateReport(distinctMatchedOrganizations);
            
            sparkAvroSaver.saveJavaRDD(sc.parallelize(reportEntries, 1), ReportEntry.SCHEMA$, outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------
    
    private static JavaPairRDD<String, CharSequence> getProjectToFundingClass(JavaSparkContext sc, String inputAvroProjectPath, String projectFundingClassWhitelistRegex) {
        SparkAvroLoader avroLoader = new SparkAvroLoader();
        JavaRDD<Project> projects = avroLoader.loadJavaRDD(sc, inputAvroProjectPath, Project.class);
        return projects.mapToPair(e -> new Tuple2<>(e.getId().toString(), e.getFundingClass())).filter(
                e -> (e._2 != null && Pattern.matches(projectFundingClassWhitelistRegex, e._2)));
    }
    
    private static Tuple2<CharSequence, CharSequence> buildMatchedOrgKey(MatchedOrganization element) {
        return new Tuple2<>(element.getDocumentId(), element.getOrganizationId());
    }
    
    private static List<AffMatchProjectOrganization> getRelationsWithSingleOrganization(Iterable<AffMatchProjectOrganization> element) {
        Iterator<AffMatchProjectOrganization> it = element.iterator();
        if (it.hasNext()) {
            AffMatchProjectOrganization result = it.next();
            if (!it.hasNext()) {
                return Collections.singletonList(result);
            }
        }
        return Collections.emptyList();
    }
    
    private static MatchedOrganization buildMatchedOrganization(Tuple2<String, Tuple2<AffMatchDocumentProject,AffMatchProjectOrganization>> element) {
        MatchedOrganization.Builder builder = MatchedOrganization.newBuilder();
        builder.setDocumentId(element._2._1.getDocumentId());
        builder.setOrganizationId(element._2._2.getOrganizationId());
        //TODO currenlty propagating doc->proj matching confidence level, maybe we should alter this score a little bit 
        builder.setMatchStrength(element._2._1.getConfidenceLevel());
        return builder.build();
    }
    
    @Parameters(separators = "=")
    private static class ProjectRelatedDocOrgMatchingJobParameters {
        
        @Parameter(names = "-inputAvroProjectPath", required = true, description="path to directory with avro files containing project entities")
        private String inputAvroProjectPath;
        
        @Parameter(names = "-inputAvroInferredDocProjPath", required = true, description="path to directory with avro files containing inferred document to project relations")
        private String inputAvroInferredDocProjPath;
        
        @Parameter(names = "-inputAvroDocProjPath", required = true, description="path to directory with avro files containing document to project relations")
        private String inputAvroDocProjPath;
        
        @Parameter(names = "-inputDocProjConfidenceThreshold", description="minimal confidence level for document to project relations (no limit by default)")
        private Float inputDocProjConfidenceThreshold = null;
        
        @Parameter(names = "-projectFundingClassWhitelistRegex", description="regex accepting project references by funding class")
        private String projectFundingClassWhitelistRegex;
        
        @Parameter(names = "-inputAvroProjOrgPath", required = true, description="path to directory with avro files containing project to organization relations")
        private String inputAvroProjOrgPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputAvroReportPath", required = true, description="path to a directory with the execution result report")
        private String outputAvroReportPath;
    }
    
}
