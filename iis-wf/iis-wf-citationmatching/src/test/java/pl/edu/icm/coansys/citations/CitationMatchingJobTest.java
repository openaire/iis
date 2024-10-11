package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.io.Files;

import pl.edu.icm.coansys.citations.data.TextWithBytesWritable;
import pl.edu.icm.coansys.commons.hadoop.LocalSequenceFileUtils;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * @author madryk
 */
public class CitationMatchingJobTest {


    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
        
        
    @BeforeEach
    public void before() {
        
        workingDir = Files.createTempDir();
        
    }
    
    
    @AfterEach
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingJob() throws IOException {
        
        
        // given
        
        String citationPath = "src/test/resources/heuristic/citations";
        
        String documentPath = "src/test/resources/heuristic/documents";
        
        String outputDirPath = workingDir.getAbsolutePath() + "/output";
 
        SparkJob sparkJob = createCitationMatchingJob(citationPath, documentPath, outputDirPath);
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        assertMatchedCitations(outputDirPath, "src/test/resources/matched_output");
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob createCitationMatchingJob(String citationPath, String documentPath, String outputDirPath) {
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName("CitationMatchingJob")
        
                                           .setMainClass(CitationMatchingJob.class)
                                           .addArg("-citationPath", citationPath)
                                           .addArg("-documentPath", documentPath)
                                           
                                           .addArg("-outputDirPath", outputDirPath)
                                           
                                           .addArg("-numberOfPartitions", "5")
                                           
                                           .build();
        return sparkJob;
    }
    
    
    private void assertMatchedCitations(String outputDirPath, String expectedMatchedCitationDirPath) throws IOException {
        
        List<Pair<TextWithBytesWritable, Text>> actualMatchedCitations = LocalSequenceFileUtils.readSequenceFile(new File(outputDirPath), TextWithBytesWritable.class, Text.class);
        
        List<Pair<TextWithBytesWritable, Text>> expectedMatchedCitations = LocalSequenceFileUtils.readSequenceFile(new File(expectedMatchedCitationDirPath), TextWithBytesWritable.class, Text.class);
        
        assertEquals(expectedMatchedCitations.size(), actualMatchedCitations.size());
        
        for (Pair<TextWithBytesWritable, Text> actualCitationDocIdPair : actualMatchedCitations) {
            assertTrue(isInMatchedCitations(expectedMatchedCitations, actualCitationDocIdPair));
        }
        
    }
    
    
    private boolean isInMatchedCitations(List<Pair<TextWithBytesWritable, Text>> citations, Pair<TextWithBytesWritable, Text> citationIdDocPair) {
        
        for (Pair<TextWithBytesWritable, Text> citIdDocPair : citations) {
            if (citIdDocPair.getKey().text().equals(citationIdDocPair.getKey().text())) {
                return Arrays.equals(citIdDocPair.getKey().bytes().copyBytes(), citationIdDocPair.getKey().bytes().copyBytes())
                        && citIdDocPair.getValue().equals(citationIdDocPair.getValue());
            }
        }
        
        return false;
        
    }
    
}
