package eu.dnetlib.iis.wf.metadataextraction;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.security.InvalidParameterException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;

import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.report.test.ValueSpecMatcher;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import pl.edu.icm.cermine.ContentExtractor;

/**
 * Metadata extractor main class executing extraction for all files provided as arguments.
 * @author mhorst
 *
 */
public class MetadataExtractorMain {

    
    private static final long interruptionDefaultTimeoutSecs = 600;
    
    private static final Logger log = Logger.getLogger(MetadataExtractorMain.class);
    
    
    private static final String fileNameSuffixContent = ".pdf";
    
    private static final String fileNameSuffixExpectations = ".expectations";
    
    
    private static final String expectationTimeoutSecs = "timeout.secs";
    
    private static final String expectationExceptionClass = "exception.class";
    
    private static final String expectationExceptionMessage = "exception.message";
    
    private static final String expectationMetadataPrefix = "metadata.";
    
    
    private static final ValueSpecMatcher valueMatcher = new ValueSpecMatcher();
    
    
  //------------------------ LOGIC --------------------------
    
	public static void main(String[] args) throws Exception {
		if (args.length>0) {
			for (String fileLoc : args) {
			    File rootFile = new File(fileLoc);
			    if (rootFile.exists()) {
			        process(rootFile);    
			    } else {
			        throw new InvalidParameterException("Location does not exist: " + fileLoc);
			    }
			    
			}
		} else {
			throw new InvalidParameterException("no pdf file path provided");
		}
	}
	
	//------------------------ PRIVATE --------------------------
	
	/**
	 * Handles file or directory.
	 */
	private static final void process(File file) throws Exception {
	    if (file.isDirectory()) {
            File[] files = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isDirectory() || pathname.getName().toLowerCase().endsWith(fileNameSuffixContent);
                }
            });
            for (File currentFile : files) {
                process(currentFile);
            }
        } else {
            processFile(file);
        }
	}
	
	/**
	 * Handles PDF file and optional expectations file.
	 */
	private static final void processFile(File file) throws Exception {
	    log.info("processing file: " + file);
	    
	    long timeout = interruptionDefaultTimeoutSecs;
	    
//	    looking for optional expectations
	    Properties expectations = readExpectations(new File(generateExpectationsFileName(file)));
        if (expectations.getProperty(expectationTimeoutSecs)!=null) {
            timeout = Long.parseLong(expectations.getProperty(expectationTimeoutSecs));
            log.info("overriding default timeout: " + interruptionDefaultTimeoutSecs + " with new value: " + timeout + " [secs]");
        }
	    
        InputStream inputStream = new FileInputStream(file);
        ExtractedDocumentMetadata extractedMetadata = null;
        try {
            ContentExtractor extractor = new ContentExtractor(timeout);
            extractor.setPDF(inputStream);
            Element resultElem = extractor.getContentAsNLM();
            String rawText = extractor.getRawFullText();
            extractedMetadata = NlmToDocumentWithBasicMetadataConverter.convertFull(
                    generateId(file), new Document(resultElem), rawText);
            validateContent(extractedMetadata, expectations);
            
        } catch (UnmetExpectationException e) {
            log.error("expectations were unmet for record:\n" + JsonUtils.toPrettyJSON(extractedMetadata.toString()));
            throw e;
            
        } catch (Exception e) {
            handleException(e, expectations);
           
        } finally {
            inputStream.close();
        }
	}
	
	/**
	 * Generates id based on file name.
	 */
	private static final String generateId(File file) {
	    return file.getName().substring(0, file.getName().lastIndexOf('.'));
	}
	
	/**
	 * Generates expectations file absolute path based on content file name by replacing file extension.
	 */
	private static final String generateExpectationsFileName(File contentFile) {
	    return contentFile.getAbsolutePath().substring(0, contentFile.getAbsolutePath().lastIndexOf('.')) + fileNameSuffixExpectations;
	}
	
	/**
	 * Reads whole set of expectations defined as properties.
	 */
	private static final Properties readExpectations(File file) throws IOException {
	    Properties expecations = new Properties();
	    if (file.exists()) {
	        expecations.load(new FileInputStream(file));
	    }
	    return expecations;
	}
	
	/**
     * Validates record fields against specified expectations. RuntimeException is thrown when invalid.
     * 
     * @param extractedMetadata metadata record to be validated
     * @param expectations set of field expectations defined as properties where key is field location (prefixed with 'metadata.') and value is expected value
     */
    private static void validateContent(ExtractedDocumentMetadata extractedMetadata, Properties expectations) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, UnmetExpectationException {
        for (Entry<Object, Object> fieldExpectation : expectations.entrySet()) {
            String keyCandidate = (String)fieldExpectation.getKey();
            if (keyCandidate.startsWith(expectationMetadataPrefix)) {
                String fieldPath = keyCandidate.substring(expectationMetadataPrefix.length());
                String currentValue = PropertyUtils.getNestedProperty(extractedMetadata, fieldPath).toString();
                String expectedValue = fieldExpectation.getValue().toString();
                if (!valueMatcher.matches(currentValue, expectedValue)) {
                    throw new UnmetExpectationException("expectation not met: invalid field value for path: " + fieldPath + 
                            ", expected: '" + fieldExpectation.getValue() + "', " + "got: '" + currentValue + "'");
                }
            }
        }
    }
	
	/**
	 * Handles exception by validating against exception expectations if any defined.
	 * When expectations are not defined or conditions are not met exception is rethrown.
	 */
	private static void handleException(Exception e, Properties expectations) throws Exception {
	    if (expectations.containsKey(expectationExceptionClass)) {
	        String expectedExceptionClass = expectations.getProperty(expectationExceptionClass);
            if (!expectedExceptionClass.equals(e.getClass().getName())) {
                throwExpectationNotMet(expectationExceptionClass, expectations, e.getClass().getName(), e);
            } else {
                logExpectationMet(expectationExceptionClass, expectations);
            }
            String optionalExceptionMessage = expectations.getProperty(expectationExceptionMessage);
            if (optionalExceptionMessage != null) {
                if (!optionalExceptionMessage.equals(e.getMessage())) {
                    throwExpectationNotMet(expectationExceptionMessage, expectations, e.getMessage(), e);
                } else {
                    logExpectationMet(expectationExceptionMessage, expectations);    
                }
            }
	    } else {
	        log.error("expectation '" + expectationExceptionClass + "' was not defined but exception occured while handling content, interrupting!");
	        throw e;	        
	    }
	}
	
	/**
	 * Logs information about met expectation.
	 */
	private static void logExpectationMet(String expectationName, Properties expectations) {
	    log.info("expectation met, name: '" + expectationName + "', expected value: '" + expectations.getProperty(expectationName) + "'");
	}
	
	/**
	 * Throws {@link UnmetExpectationException} with unmet expectation details.
	 * @throws UnmetExpectationException 
	 */
	private static void throwExpectationNotMet(String expectationName, Properties expectations, String receivedValue, Exception e) throws UnmetExpectationException {
	    throw new UnmetExpectationException("expecatation not met, name: '" + expectationName + "', expected value: '" + 
	            expectations.getProperty(expectationName) + "', got: '" + receivedValue + "'", e);
    }
	
}
