package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.List;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
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

    private String readFundingTree() {
        return ClassPathResourceProvider
                .getResourceContent("/eu/dnetlib/iis/wf/importer/converter/fundingclass_example.xml");
    }
}
