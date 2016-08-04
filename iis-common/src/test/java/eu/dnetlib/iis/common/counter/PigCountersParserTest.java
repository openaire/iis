package eu.dnetlib.iis.common.counter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;

/**
 * @author madryk
 */
public class PigCountersParserTest {

    private PigCountersParser pigCountersParser = new PigCountersParser();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void parse() throws IOException {
        
        // given
        
        String pigCountersJson = readFileFromClasspath("/eu/dnetlib/iis/common/report/pigCounters.json");
        
        // execute
        
        PigCounters pigCounters = pigCountersParser.parse(pigCountersJson);
        
        // assert
        
        Map<String, String> rootLevelCounters = pigCounters.getRootLevelCounters();
        
        assertEquals(15, rootLevelCounters.size());
        
        assertEquals("4", rootLevelCounters.get("NUMBER_JOBS"));
        assertEquals("0", rootLevelCounters.get("SMM_SPILL_COUNT"));
        assertEquals("0", rootLevelCounters.get("RETURN_CODE"));
        assertEquals("job_1467867518322_6688,job_1467867518322_6687,job_1467867518322_6689,job_1467867518322_6690", rootLevelCounters.get("JOB_GRAPH"));
        assertEquals("3", rootLevelCounters.get("RECORD_WRITTEN"));
        assertEquals("-1", rootLevelCounters.get("ERROR_CODE"));
        
        assertEquals("HASH_JOIN,DISTINCT,FILTER", rootLevelCounters.get("FEATURES"));
        assertEquals("1bbda10f-3149-49de-9798-77ec97b5e3e1", rootLevelCounters.get("SCRIPT_ID"));
        assertEquals("0.12.0-cdh5.5.2", rootLevelCounters.get("PIG_VERSION"));
        assertEquals("3870", rootLevelCounters.get("BYTES_WRITTEN"));
        assertEquals("0", rootLevelCounters.get("PROACTIVE_SPILL_COUNT_RECORDS"));
        assertEquals("0", rootLevelCounters.get("PROACTIVE_SPILL_COUNT_OBJECTS"));
        assertEquals("2.6.0-cdh5.5.2", rootLevelCounters.get("HADOOP_VERSION"));
        assertEquals("92829", rootLevelCounters.get("DURATION"));
        assertEquals("PIG", rootLevelCounters.get("ACTION_TYPE"));
        
        
        
        assertThat(pigCounters.getJobIds(), containsInAnyOrder(
                "job_1467867518322_6687", "job_1467867518322_6688",
                "job_1467867518322_6689", "job_1467867518322_6690"));
        
        JobCounters jobCounters1 = pigCounters.getJobCounters("job_1467867518322_6687");
        assertThat(jobCounters1.getAliases(), containsInAnyOrder("cachedDocumentId", "documentMeta"));
        assertThat(jobCounters1.getCountersCount(), is(21));
        
        assertThat(jobCounters1.getCounter("JOB_ID"), is("job_1467867518322_6687"));
        assertThat(jobCounters1.getCounter("Alias"), is("cachedDocumentId,documentMeta"));
        
        assertThat(jobCounters1.getCounter("NUMBER_MAPS"), is("1"));
        assertThat(jobCounters1.getCounter("MAP_INPUT_RECORDS"), is("3"));
        assertThat(jobCounters1.getCounter("MAP_OUTPUT_RECORDS"), is("3"));
        assertThat(jobCounters1.getCounter("MIN_MAP_TIME"), is("3149"));
        assertThat(jobCounters1.getCounter("MAX_MAP_TIME"), is("3149"));
        assertThat(jobCounters1.getCounter("AVG_MAP_TIME"), is("3149"));
        
        assertThat(jobCounters1.getCounter("NUMBER_REDUCES"), is("1"));
        assertThat(jobCounters1.getCounter("REDUCE_INPUT_RECORDS"), is("3"));
        assertThat(jobCounters1.getCounter("REDUCE_OUTPUT_RECORDS"), is("0"));
        assertThat(jobCounters1.getCounter("MIN_REDUCE_TIME"), is("2806"));
        assertThat(jobCounters1.getCounter("MAX_REDUCE_TIME"), is("2806"));
        assertThat(jobCounters1.getCounter("AVG_REDUCE_TIME"), is("2806"));
        
        assertThat(jobCounters1.getCounter("RECORD_WRITTEN"), is("0"));
        assertThat(jobCounters1.getCounter("BYTES_WRITTEN"), is("0"));
        assertThat(jobCounters1.getCounter("HDFS_BYTES_WRITTEN"), is("737"));
        
        assertThat(jobCounters1.getCounter("SMMS_SPILL_COUNT"), is("0"));
        assertThat(jobCounters1.getCounter("PROACTIVE_SPILL_COUNT_RECS"), is("0"));
        assertThat(jobCounters1.getCounter("PROACTIVE_SPILL_COUNT_OBJECTS"), is("0"));
        
        assertThat(jobCounters1.getCounter("FEATURE"), is("DISTINCT,MULTI_QUERY"));
        
        
        JobCounters jobCounters2 = pigCounters.getJobCounters("job_1467867518322_6688");
        assertThat(jobCounters2.getAliases(), containsInAnyOrder("documentContent", "documentContentId"));
        assertThat(jobCounters2.getCountersCount(), is(21));
        
        
        JobCounters jobCounters3 = pigCounters.getJobCounters("job_1467867518322_6689");
        assertThat(jobCounters3.getAliases(), containsInAnyOrder("documentContentFiltered", "joinedDocumentContent", "joinedFilteredDocumentContent"));
        assertThat(jobCounters3.getCountersCount(), is(21));
        
        
        JobCounters jobCounters4 = pigCounters.getJobCounters("job_1467867518322_6690");
        assertThat(jobCounters4.getAliases(), containsInAnyOrder("documentMetaFiltered", "joinedDocumentMeta"));
        assertThat(jobCounters4.getCountersCount(), is(21));
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private String readFileFromClasspath(String classpath) throws IOException {
        
        try (InputStream input = getClass().getResourceAsStream(classpath)) {
            return IOUtils.toString(input);
        }
        
    }
}
