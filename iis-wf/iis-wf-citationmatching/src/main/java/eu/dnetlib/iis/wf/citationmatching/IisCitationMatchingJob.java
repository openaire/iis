package eu.dnetlib.iis.wf.citationmatching;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import pl.edu.icm.coansys.citations.ConfigurableCitationMatchingService;
import pl.edu.icm.coansys.citations.CoreCitationMatchingService;
import pl.edu.icm.coansys.citations.CoreCitationMatchingSimpleFactory;


/**
 * Citation matching job
 * 
 * @author madryk
 */
public class IisCitationMatchingJob {

    private static CoreCitationMatchingSimpleFactory coreCitationMatchingFactory = new CoreCitationMatchingSimpleFactory();
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        IisCitationMatchingJobParameters params = new IisCitationMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.coansys.citations.MatchableEntityKryoRegistrator");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            ConfigurableCitationMatchingService<String, ReferenceMetadata, String, DocumentMetadata, Citation, NullWritable> citationMatchingService = createConfigurableCitationMatchingService(sc, params);
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputDirPath);
            
            citationMatchingService.matchCitations(sc, params.fullDocumentPath, params.fullDocumentPath, params.outputDirPath);
            
           
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static ConfigurableCitationMatchingService<String, ReferenceMetadata, String, DocumentMetadata, Citation, NullWritable> createConfigurableCitationMatchingService(JavaSparkContext sc, IisCitationMatchingJobParameters params) {
        
        ConfigurableCitationMatchingService<String, ReferenceMetadata, String, DocumentMetadata, Citation, NullWritable> configurableCitationMatchingService = new ConfigurableCitationMatchingService<>();
        
        
        CoreCitationMatchingService coreCitationMatchingService = coreCitationMatchingFactory.createCoreCitationMatchingService(sc, params.maxHashBucketSize);
        
        
        configurableCitationMatchingService.setCoreCitationMatchingService(coreCitationMatchingService);
        configurableCitationMatchingService.setNumberOfPartitions(params.numberOfPartitions);
        
        
        ReferenceMetadataInputReader referenceMetadataInputReader = new ReferenceMetadataInputReader();
        ReferenceMetadataInputConverter referenceMetadataInputConverter = new ReferenceMetadataInputConverter();
        configurableCitationMatchingService.setInputCitationReader(referenceMetadataInputReader);
        configurableCitationMatchingService.setInputCitationConverter(referenceMetadataInputConverter);
        
        
        DocumentMetadataInputReader documentMetadataInputReader = new DocumentMetadataInputReader();
        DocumentMetadataInputConverter documentMetadataInputConverter = new DocumentMetadataInputConverter();
        configurableCitationMatchingService.setInputDocumentReader(documentMetadataInputReader);
        configurableCitationMatchingService.setInputDocumentConverter(documentMetadataInputConverter);
        
        
        CitationMatchingReporter citationMatchingReporter = new CitationMatchingReporter();
        citationMatchingReporter.setSparkContext(sc);
        citationMatchingReporter.setReportPath(params.outputReportPath);
        
        CitationOutputConverter citationOutputConverter = new CitationOutputConverter();
        CitationOutputWriter citationOutputWriter = new CitationOutputWriter();
        citationOutputWriter.setCitationMatchingReporter(citationMatchingReporter);
        
        configurableCitationMatchingService.setOutputConverter(citationOutputConverter);
        configurableCitationMatchingService.setOutputWriter(citationOutputWriter);
        
        
        return configurableCitationMatchingService;
    }
    
    
    @Parameters(separators = "=")
    private static class IisCitationMatchingJobParameters {
        
        @Parameter(names = "-fullDocumentPath", required = true, description = "path to directory/file with full documents (document with references")
        private String fullDocumentPath;
        
        @Parameter(names = "-outputDirPath", required = true, description = "path to directory with results")
        private String outputDirPath;
        
        @Parameter(names = "-outputReportPath", required = true, description = "path to directory with report")
        private String outputReportPath;
        
        @Parameter(names="-maxHashBucketSize", required = false, description = "max number of the citation-documents pairs for a given hash")
        private long maxHashBucketSize = 10000;
        
        @Parameter(names="-numberOfPartitions", required = false, description = "number of partitions used for rdds with citations and documents read from input files, if not set it will depend on the input format")
        private Integer numberOfPartitions = 5;
        
    }
}
