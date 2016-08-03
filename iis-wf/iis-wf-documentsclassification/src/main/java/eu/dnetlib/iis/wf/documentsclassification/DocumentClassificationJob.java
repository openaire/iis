package eu.dnetlib.iis.wf.documentsclassification;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportParam;
import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

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
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
          
            sc.sc().addFile(params.scriptDirPath, true);
            
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documents = avroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentMetadata> metadataRecords = documents.map(document -> converter.convert(document)).filter(metadata->StringUtils.isNotBlank(metadata.getAbstract$()));
            
            
            String scriptsDirOnWorkerNode = (sc.isLocal()) ? getScriptPath() : "scripts";
            
            JavaRDD<String> stringDocumentClasses = metadataRecords.pipe("bash " + scriptsDirOnWorkerNode + "/classify_documents.sh" + " " + scriptsDirOnWorkerNode);
            
            
            JavaRDD<DocumentToDocumentClasses> documentClasses = stringDocumentClasses.map(recordString -> 
                                            AvroGsonFactory.create().fromJson(recordString, DocumentToDocumentClasses.class)
            );

            documentClasses.cache();
            
            avroSaver.saveJavaRDD(documentClasses, DocumentToDocumentClasses.SCHEMA$, params.outputAvroPath);
            
            
            List<ReportParam> reportEntries = reportGenerator.generateReport(documentClasses);
            
            avroSaver.saveJavaRDD(sc.parallelize(reportEntries), ReportParam.SCHEMA$, params.outputReportPath);
            
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
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
    }
}
