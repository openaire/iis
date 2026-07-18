package eu.dnetlib.iis.wf.referenceextraction.covid19;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.DocumentMetadata;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.MatchedDocument;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.io.IOException;
import java.util.List;

/**
 * Covid-19 reference extraction job.
 * <br/><br/>
 * Processes documents {link ExtractedDocumentMetadataMergedWithOriginal} and calculates {@link MatchedDocument} out of them.
 * 
 * @author Marek Horst
 */
public class Covid19ReferenceExtractionJob {

    private static final String COUNTER_REFERENCES_TOTAL = "processing.referenceExtraction.covid-19.reference.total";
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        Covid19ReferenceExtractionJobParameters params = new Covid19ReferenceExtractionJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            sc.sc().addFile(params.scriptDirPath, true);

            String conceptId = params.predefinedConceptId;
            float confidenceLevel = Float.parseFloat(params.predefinedConfidenceLevel);

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> rawDocuments = avroLoader
                    .loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> repartDocuments = 
                    shouldRepartition(params.numberOfPartitions)
                    ? rawDocuments.repartition(Integer.parseInt(params.numberOfPartitions))
                    : rawDocuments;
            
            JavaRDD<DocumentMetadata> metadataRecords = repartDocuments
                    .map(document -> convertInput(document))
                    .filter(metadata->StringUtils.isNotBlank(metadata.getAbstract$()));

            // FIXME make sure the proper dir is set in the k8s env
            // FIXME is the script going to be available in the working dir anyway since the package will not be extracted on the pod?
            // FIXME should we bake the scripts into the docker image instead?
            // it is airflow specific question on how it handles the JAR and how to get to the resources nested in such jar file
            // TODO write a prompt asking for preparing the airlfow workflow definition and resolving this script location issue
            String scriptsDirOnWorkerNode = (sc.isLocal()) ? getScriptPath() : "scripts";
            
            JavaRDD<String> matchedDocumentsJson = metadataRecords
                    .pipe("bash " + scriptsDirOnWorkerNode + "/extract_references.sh" + " " + scriptsDirOnWorkerNode);

            JavaRDD<MatchedDocument> matchedDocuments = matchedDocumentsJson
                    .map(recordString -> AvroGsonFactory.create().fromJson(recordString, MatchedDocument.class)
            );

            JavaRDD<DocumentToConceptId> convertedDocuments = matchedDocuments.map(meta -> convertOutput(meta, conceptId, confidenceLevel));

            convertedDocuments.cache();
            
            avroSaver.saveJavaRDD(matchedDocuments, MatchedDocument.SCHEMA$, params.outputAvroPath);

            List<ReportEntry> reportEntries = generateReport(convertedDocuments);
            
            avroSaver.saveJavaRDD(sc.parallelize(reportEntries, 1), ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    
    /**
     * Checks whether partitioning should be perfomed based on the parameter value.
     */
    private static boolean shouldRepartition(String numberOfPartitions) {
        return (StringUtils.isNotBlank(numberOfPartitions) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(numberOfPartitions));
    }
    
    private static String getScriptPath() {
        String path = SparkFiles.get("scripts");

        if (SystemUtils.IS_OS_WINDOWS) {
            return path.replace("\\", "/");
        }
        
        return path;
    }
   
    private static DocumentMetadata convertInput(ExtractedDocumentMetadataMergedWithOriginal input) {
        DocumentMetadata.Builder metaBuilder = DocumentMetadata.newBuilder();
        metaBuilder.setId(input.getId());
        metaBuilder.setTitle(input.getTitle());
        metaBuilder.setAbstract$(input.getAbstract$());
        if (input.getYear() != null) {
            metaBuilder.setDate(String.valueOf(input.getYear()));    
        }
        return metaBuilder.build();
    }
    
    private static DocumentToConceptId convertOutput(MatchedDocument input, String conceptId, float confidenceLevel) {
        DocumentToConceptId.Builder resultBuilder = DocumentToConceptId.newBuilder();
        resultBuilder.setDocumentId(input.getId());
        resultBuilder.setConceptId(conceptId);
        resultBuilder.setConfidenceLevel(confidenceLevel);
        return resultBuilder.build();
    }

    /**
     * Generates the reference extraction execution report
     * 
     * @param results result rdd of the reference extraction job execution
     */
    private static List<ReportEntry> generateReport(JavaRDD<DocumentToConceptId> results) {
        Preconditions.checkNotNull(results);
        long totalResultsCount = results.count();
        return ImmutableList.of(createCounterReportEntry(COUNTER_REFERENCES_TOTAL, totalResultsCount));
    }

    @Parameters(separators = "=")
    private static class Covid19ReferenceExtractionJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;

        @Parameter(names = "-predefinedConceptId", required = true)
        private String predefinedConceptId;
        
        @Parameter(names = "-predefinedConfidenceLevel", required = true)
        private String predefinedConfidenceLevel;

        @Parameter(names = "-scriptDirPath", required = true, description = "path to directory with scripts")
        private String scriptDirPath;
        
        @Parameter(names = "-numberOfPartitions", required = false, description = "number of partitions the data should be sliced into")
        private String numberOfPartitions;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
    }
}