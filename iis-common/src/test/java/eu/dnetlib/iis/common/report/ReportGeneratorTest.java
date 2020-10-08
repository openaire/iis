package eu.dnetlib.iis.common.report;

import com.google.common.collect.ImmutableMap;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author madryk
 */
public class ReportGeneratorTest {

    private ReportGenerator reportGenerator = new ReportGenerator();

    @TempDir
    public File tempFolder;

    //------------------------ TESTS --------------------------
    
    @Test
    public void run() throws Exception {
        
        // given
        
        Path outputDirPath = new Path(tempFolder.getPath());
        PortBindings portBindings = new PortBindings(ImmutableMap.of(), ImmutableMap.of("report", outputDirPath));
        Configuration conf = new Configuration(false);
        
        Map<String, String> parameters = ImmutableMap.of(
                "report.group.firstParam", "11",
                "report.group.secondParam", "22",
                "notReportPrefixed.group.thirdParam", "3123");
        
        
        // execute
        
        reportGenerator.run(portBindings, conf, parameters);
        
        
        // assert
        
        List<ReportEntry> actualReportEntries = AvroTestUtils.readLocalAvroDataStore(tempFolder.getPath());
        
        assertThat(actualReportEntries, containsInAnyOrder(
                new ReportEntry("group.firstParam", ReportEntryType.COUNTER, "11"),
                new ReportEntry("group.secondParam", ReportEntryType.COUNTER, "22")));
    }
    
}
