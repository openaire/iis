package eu.dnetlib.iis.workflows.documentsclassification;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.core.common.AvroGsonFactory;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * Document classification spark job
 * <br/><br/>
 * Temporarily - only the input transformer, eventually the whole document classification job
 * 
 * @author Łukasz Dumiszewski
 */

public class DocumentClassificationJob {

    
    private static DocumentToDocClassificationMetadataConverter converter = new DocumentToDocClassificationMetadataConverter();
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        DocumentClassificationJobParameters params = new DocumentClassificationJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            sc.sc().addFile(params.scriptDirPath, true);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documents = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            
            JavaRDD<DocumentMetadata> metadataRecords = documents.map(document -> converter.convert(document)).filter(metadata->StringUtils.isNotBlank(metadata.getAbstract$()));
            
            String scriptRootDir = SparkFiles.getRootDirectory().replaceAll("\\\\", "/")+"/scripts";
            
            System.out.println(scriptRootDir);
            
            JavaRDD<String> stringDocumentClasses = metadataRecords.pipe("sh -c 'cd " + scriptRootDir + " && sh " + scriptRootDir+ "/classify_documents.sh'");
            
            JavaRDD<DocumentToDocumentClasses> documentClasses = stringDocumentClasses.map(recordString -> AvroGsonFactory.create().fromJson(recordString, DocumentToDocumentClasses.class));
            
            System.out.println(documentClasses.collect().toString());
            
            SparkAvroSaver.saveJavaRDD(documentClasses, DocumentToDocumentClasses.SCHEMA$, params.outputAvroPath);
        
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
   @Parameters(separators = "=")
    private static class DocumentClassificationJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-scriptDirPath", required = true, description = "path to directory with scripts")
        private String scriptDirPath;
        
        
    }
}
