package pl.edu.icm.coansys.citations;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Service for matching citations. It can be configured
 * to use different citation and document readers or
 * matched citation writers
 * 
 * @author madryk
 */
public class ConfigurableCitationMatchingService<INPUT_CIT_KEY, INPUT_CIT_VALUE, INPUT_DOC_KEY, INPUT_DOC_VALUE, OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> {

    private InputCitationReader<INPUT_CIT_KEY, INPUT_CIT_VALUE> inputCitationReader;
    
    private InputCitationConverter<INPUT_CIT_KEY, INPUT_CIT_VALUE> inputCitationConverter;
    
    
    private InputDocumentReader<INPUT_DOC_KEY, INPUT_DOC_VALUE> inputDocumentReader;
    
    private InputDocumentConverter<INPUT_DOC_KEY, INPUT_DOC_VALUE> inputDocumentConverter;
    
    
    private OutputConverter<OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> outputConverter;
    
    private OutputWriter<OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> outputWriter;
    
    
    private CoreCitationMatchingService coreCitationMatchingService;
    
    
    private Integer numberOfPartitions;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads citations and documents from specified paths and invokes citation matching algorithm 
     * using {@link CoreCitationMatchingService#matchCitations(JavaPairRDD, JavaPairRDD)}.
     * After that it saves matched citations to path specified by last argument.
     */
    public void matchCitations(JavaSparkContext sparkContext, String inputCitationPath, String inputDocumentPath, String outputPath) {
        
        JavaPairRDD<INPUT_CIT_KEY, INPUT_CIT_VALUE> citations = inputCitationReader.readCitations(sparkContext, inputCitationPath);
        JavaPairRDD<String, MatchableEntity> citationsConverted = inputCitationConverter.convertCitations(citations);
        citationsConverted = citationsConverted.partitionBy(new HashPartitioner(numberOfPartitions));
        
        JavaPairRDD<INPUT_DOC_KEY, INPUT_DOC_VALUE> documents = inputDocumentReader.readDocuments(sparkContext, inputDocumentPath);
        JavaPairRDD<String, MatchableEntity> documentsConverted = inputDocumentConverter.convertDocuments(documents);
        documentsConverted = documentsConverted.partitionBy(new HashPartitioner(numberOfPartitions));
        
        
        JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations = coreCitationMatchingService.matchCitations(citationsConverted, documentsConverted);
        
        
        JavaPairRDD<OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> matchedCitationsConverted = outputConverter.convertMatchedCitations(matchedCitations);
        outputWriter.writeMatchedCitations(matchedCitationsConverted, outputPath);
    }


    //------------------------ SETTERS --------------------------
    
    public void setInputCitationReader(InputCitationReader<INPUT_CIT_KEY, INPUT_CIT_VALUE> inputCitationReader) {
        this.inputCitationReader = inputCitationReader;
    }

    public void setInputCitationConverter(InputCitationConverter<INPUT_CIT_KEY, INPUT_CIT_VALUE> inputCitationConverter) {
        this.inputCitationConverter = inputCitationConverter;
    }

    public void setInputDocumentReader(InputDocumentReader<INPUT_DOC_KEY, INPUT_DOC_VALUE> inputDocumentReader) {
        this.inputDocumentReader = inputDocumentReader;
    }

    public void setInputDocumentConverter(InputDocumentConverter<INPUT_DOC_KEY, INPUT_DOC_VALUE> inputDocumentConverter) {
        this.inputDocumentConverter = inputDocumentConverter;
    }

    public void setOutputConverter(OutputConverter<OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> outputConverter) {
        this.outputConverter = outputConverter;
    }

    public void setOutputWriter(OutputWriter<OUTPUT_MATCHED_KEY, OUTPUT_MACHED_VALUE> outputWriter) {
        this.outputWriter = outputWriter;
    }

    public void setCoreCitationMatchingService(CoreCitationMatchingService coreCitationMatchingService) {
        this.coreCitationMatchingService = coreCitationMatchingService;
    }

    public void setNumberOfPartitions(Integer numberOfPartitions) {
        this.numberOfPartitions = numberOfPartitions;
    }
    
    
}
