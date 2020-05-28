package eu.dnetlib.iis.wf.referenceextraction.patent.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

/**
 * {@link OpsPatentMetadataXPathBasedParser} test class.
 * 
 * @author mhorst
 *
 */
public class OpsPatentMetadataXPathBasedParserTest {

    static final String xmlResourcesRootClassPath = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/";

    private OpsPatentMetadataXPathBasedParser parser = new OpsPatentMetadataXPathBasedParser();

    @Test
    public void testExtractMetadataFromValidXMLfile() throws Exception {
        // given
        String xmlContents = IOUtils.toString(OpsPatentMetadataXPathBasedParser.class
                .getResourceAsStream(xmlResourcesRootClassPath + "WO.0042078.A1.xml"), StandardCharsets.UTF_8.name());

        // execute
        Patent.Builder patent = parser.parse(xmlContents, Patent.newBuilder());

        // assert
        assertNotNull(patent);
        assertTrue(patent.hasApplnTitle());
        assertEquals("AQUEOUS PEROXIDE EMULSIONS", patent.getApplnTitle());
        assertNotNull(patent.getApplnAbstract());
        assertEquals("Simple abstract in English", patent.getApplnAbstract());
        assertNotNull(patent.getIpcClassSymbol());
        assertEquals(1, patent.getIpcClassSymbol().size());
        assertEquals("C08F2/20", patent.getIpcClassSymbol().get(0));
        assertEquals("WO2000EP00003", patent.getApplnNrEpodoc());
        assertEquals("2000-01-06", patent.getApplnFilingDate());
        assertEquals("2000-07-20", patent.getEarliestPublnDate());

        assertNotNull(patent.getApplicantNames());
        assertEquals(3, patent.getApplicantNames().size());
        assertEquals("AKZO NOBEL N.V", patent.getApplicantNames().get(0));
        assertEquals("WESTMIJZE, HANS", patent.getApplicantNames().get(1));
        assertEquals("O, BOEN, HO", patent.getApplicantNames().get(2));

        assertNotNull(patent.getApplicantCountryCodes());
        assertEquals(3, patent.getApplicantCountryCodes().size());
        assertEquals("NL", patent.getApplicantCountryCodes().get(0));
        assertEquals("NL", patent.getApplicantCountryCodes().get(1));
        assertEquals("NL", patent.getApplicantCountryCodes().get(2));
    }

}
