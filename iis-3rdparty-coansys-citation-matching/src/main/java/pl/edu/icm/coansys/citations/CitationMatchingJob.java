package pl.edu.icm.coansys.citations;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * 
 * Citation matching job.
 * 
 * @author Łukasz Dumiszewski
 */

public class CitationMatchingJob {
    
    private static CoreCitationMatchingSimpleFactory coreCitationMatchingFactory = new CoreCitationMatchingSimpleFactory();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        CitationMatchingJobParameters params = new CitationMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.coansys.citations.MatchableEntityKryoRegistrator");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            ConfigurableCitationMatchingService<?,?,?,?,?,?> citationMatchingService = createConfigurableCitationMatchingService(sc, params);
            
            citationMatchingService.matchCitations(sc, params.citationPath, params.documentPath, params.outputDirPath);
            
           
        }
        
    }



    //------------------------ PRIVATE --------------------------
    
    private static ConfigurableCitationMatchingService<?,?,?,?,?,?> createConfigurableCitationMatchingService(JavaSparkContext sc, CitationMatchingJobParameters params) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        ConfigurableCitationMatchingService<?,?,?,?,?,?> configurableCitationMatchingService = new ConfigurableCitationMatchingService<>();
        
        CoreCitationMatchingService coreCitationMatchingService = coreCitationMatchingFactory.createCoreCitationMatchingService(sc, params.maxHashBucketSize);
        
        configurableCitationMatchingService.setCoreCitationMatchingService(coreCitationMatchingService);
        configurableCitationMatchingService.setNumberOfPartitions(params.numberOfPartitions);
        
        createAndSetInputCitationReaderAndConverter(configurableCitationMatchingService, sc, params);
        createAndSetInputDocumentReaderAndConverter(configurableCitationMatchingService, sc, params);
        createAndSetOutputWriterAndConverter(configurableCitationMatchingService, sc, params);
        
        return configurableCitationMatchingService;
    }

    private static <ICK, ICV, IDK, IDV, OMK, OMV> void createAndSetInputCitationReaderAndConverter(ConfigurableCitationMatchingService<ICK, ICV, IDK, IDV, OMK, OMV> citationMatchingService, JavaSparkContext sc, CitationMatchingJobParameters params) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        @SuppressWarnings("unchecked")
        InputCitationReader<ICK, ICV> inputCitationReader = Class.forName(params.inputCitationReaderClass).asSubclass(InputCitationReader.class).newInstance();
        citationMatchingService.setInputCitationReader(inputCitationReader);
        
        @SuppressWarnings("unchecked")
        InputCitationConverter<ICK, ICV>  inputCitationConverter = Class.forName(params.inputCitationConverterClass).asSubclass(InputCitationConverter.class).newInstance();
        citationMatchingService.setInputCitationConverter(inputCitationConverter);
        
    }

    private static <ICK, ICV, IDK, IDV, OMK, OMV> void createAndSetInputDocumentReaderAndConverter(ConfigurableCitationMatchingService<ICK, ICV, IDK, IDV, OMK, OMV> citationMatchingService, JavaSparkContext sc, CitationMatchingJobParameters params) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        @SuppressWarnings("unchecked")
        InputDocumentReader<IDK, IDV> inputDocumentReader = Class.forName(params.inputDocumentReaderClass).asSubclass(InputDocumentReader.class).newInstance();
        citationMatchingService.setInputDocumentReader(inputDocumentReader);
        
        @SuppressWarnings("unchecked")
        InputDocumentConverter<IDK, IDV> inputDocumentConverter = Class.forName(params.inputDocumentConverterClass).asSubclass(InputDocumentConverter.class).newInstance();
        citationMatchingService.setInputDocumentConverter(inputDocumentConverter);
        
    }

    private static <ICK, ICV, IDK, IDV, OMK, OMV> void createAndSetOutputWriterAndConverter(ConfigurableCitationMatchingService<ICK, ICV, IDK, IDV, OMK, OMV> citationMatchingService, JavaSparkContext sc, CitationMatchingJobParameters params) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        @SuppressWarnings("unchecked")
        OutputWriter<OMK, OMV> outputWriter = Class.forName(params.outputWriterClass).asSubclass(OutputWriter.class).newInstance();
        citationMatchingService.setOutputWriter(outputWriter);
        
        @SuppressWarnings("unchecked")
        OutputConverter<OMK, OMV> outputConverter = Class.forName(params.outputConverterClass).asSubclass(OutputConverter.class).newInstance();
        citationMatchingService.setOutputConverter(outputConverter);
        
    }
    
    
    @Parameters(separators = "=")
    private static class CitationMatchingJobParameters {
        
        @Parameter(names = "-citationPath", required = true, description = "path to directory/file with citations")
        private String citationPath;
        
        @Parameter(names = "-documentPath", required = true, description = "path to directory/file with documents")
        private String documentPath;
        
        @Parameter(names = "-outputDirPath", required = true, description = "path to directory with results")
        private String outputDirPath;
        
        @Parameter(names="-maxHashBucketSize", required = false, description = "max number of the citation-documents pairs for a given hash")
        private long maxHashBucketSize = 10000;
        
        @Parameter(names="-numberOfPartitions", required = false, description = "number of partitions used for rdds with citations and documents read from input files, if not set it will depend on the input format")
        private Integer numberOfPartitions;
        
        
        @Parameter(names = "-inputDocumentReaderClass", required = false)
        private String inputDocumentReaderClass = DefaultInputReader.class.getName();
        
        @Parameter(names = "-inputDocumentConverterClass", required = false)
        private String inputDocumentConverterClass = DummyInputConverter.class.getName();
        
        
        @Parameter(names = "-inputCitationReaderClass", required = false)
        private String inputCitationReaderClass = DefaultInputReader.class.getName();
        
        @Parameter(names = "-inputCitationConverterClass", required = false)
        private String inputCitationConverterClass = DummyInputConverter.class.getName();
        
        
        @Parameter(names = "-outputWriterClass", required = false)
        private String outputWriterClass = DefaultOutputWriter.class.getName();
        
        @Parameter(names = "-outputConverterClass", required = false)
        private String outputConverterClass = DummyOutputConverter.class.getName();
    }
    
    
}
