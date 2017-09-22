package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class FundingTreeParserTest {

    FundingTreeParser parser = new FundingTreeParser();
    
    @Test
    public void testFundingClassExtraction() throws Exception {
        List<String> fundingTreeList = Collections.singletonList(readFundingTree());
        FundingDetails fundingDetails = parser.extractFundingDetails(fundingTreeList);
        assertNotNull(fundingDetails);
        assertEquals("WT", fundingDetails.getFunderShortName());
        assertNotNull(fundingDetails.getFundingLevelNames());
        assertEquals(2, fundingDetails.getFundingLevelNames().size());
        assertEquals("WT", fundingDetails.getFundingLevelNames().get(0));
        assertEquals("Non-Stream Activity - GM", fundingDetails.getFundingLevelNames().get(1));
    }
    
    @Test
    public void testFundingClassExtractionEmptyInput() throws Exception {
        List<String> fundingTreeList = Collections.emptyList();
        FundingDetails fundingDetails = parser.extractFundingDetails(fundingTreeList);
        assertNull(fundingDetails);
    }
    
    // ------------------------ PRIVATE --------------------------

    private String readFundingTree() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/iis/wf/importer/converter/fundingclass_example.xml"), "utf8");
    }
}
