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

    private static final String FUNDING_CLASS = "WT::WT";
    
    FundingTreeParser parser = new FundingTreeParser();
    
    @Test
    public void testFundingClassExtraction() throws Exception {
        List<String> fundingTreeList = Collections.singletonList(readFundingTree());
        String fundingClass = parser.extractFundingClass(fundingTreeList);
        assertNotNull(fundingClass);
        assertEquals(FUNDING_CLASS, fundingClass);
        
    }
    
    @Test
    public void testFundingClassExtractionEmptyInput() throws Exception {
        List<String> fundingTreeList = Collections.emptyList();
        String fundingClass = parser.extractFundingClass(fundingTreeList);
        assertNull(fundingClass);
    }
    
    // ------------------------ PRIVATE --------------------------

    private String readFundingTree() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/iis/wf/importer/converter/fundingclass_example.xml"), "utf8");
    }
}
