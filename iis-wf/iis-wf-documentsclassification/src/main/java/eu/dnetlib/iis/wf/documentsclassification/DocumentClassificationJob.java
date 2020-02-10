package eu.dnetlib.iis.wf.documentsclassification;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
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
 * Document classification spark job.
 * <br/><br/>
 * Processes documents {link ExtractedDocumentMetadataMergedWithOriginal} and calculates {@link DocumentToDocumentClasses} out of them.
 * 
 * @author Łukasz Dumiszewski
 */
public class DocumentClassificationJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static DocumentToDocClassificationMetadataConverter converter = new DocumentToDocClassificationMetadataConverter();
    
    private static DocClassificationReportGenerator reportGenerator = new DocClassificationReportGenerator();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        DocumentClassificationJobParameters params = new DocumentClassificationJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            sc.sc().addFile(params.scriptDirPath, true);

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> rawDocuments = avroLoader
                    .loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> repartDocuments = 
                    (StringUtils.isNotBlank(params.numberOfPartitions) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.numberOfPartitions))
                    ? rawDocuments.repartition(Integer.parseInt(params.numberOfPartitions))
                    : rawDocuments;
            
            JavaRDD<DocumentMetadata> metadataRecords = repartDocuments
                    .map(document -> converter.convert(document))
                    .filter(metadata->StringUtils.isNotBlank(metadata.getAbstract$()));

            String scriptsDirOnWorkerNode = (sc.isLocal()) ? getScriptPath() : "scripts";
            
            JavaRDD<String> stringDocumentClasses = metadataRecords
                    .pipe("bash " + scriptsDirOnWorkerNode + "/classify_documents.sh" + " " + scriptsDirOnWorkerNode);

            JavaRDD<DocumentToDocumentClasses> documentClasses = stringDocumentClasses
                    .map(recordString -> AvroGsonFactory.create().fromJson(recordString, DocumentToDocumentClasses.class)
            );

            documentClasses.cache();
            
            avroSaver.saveJavaRDD(documentClasses, DocumentToDocumentClasses.SCHEMA$, params.outputAvroPath);

            List<ReportEntry> reportEntries = reportGenerator.generateReport(documentClasses);
            
            avroSaver.saveJavaRDD(sc.parallelize(reportEntries), ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static String getScriptPath() {
        String path = SparkFiles.get("scripts");

        if (SystemUtils.IS_OS_WINDOWS) {
            return path.replace("\\", "/");
        }
        
        return path;
    }

    @Parameters(separators = "=")
    private static class DocumentClassificationJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;

        @Parameter(names = "-scriptDirPath", required = true, description = "path to directory with scripts")
        private String scriptDirPath;
        
        @Parameter(names = "-numberOfPartitions", required = false, description = "number of partitions the data should be sliced into")
        private String numberOfPartitions;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
    }
}
