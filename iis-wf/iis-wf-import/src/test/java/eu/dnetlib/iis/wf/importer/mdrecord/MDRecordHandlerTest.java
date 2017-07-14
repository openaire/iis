package eu.dnetlib.iis.wf.importer.mdrecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Test;

import eu.dnetlib.iis.wf.importer.dataset.DataciteXmlImporterTest;

/**
 * @author mhorst
 *
 */
public class MDRecordHandlerTest {

    @Test
    public void testImport() throws Exception {
        InputStream inputStream = null;
        SAXParser saxParser = null;
        try {
            SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
            saxParserFactory.setNamespaceAware(true);
            saxParser = saxParserFactory.newSAXParser();
            MDRecordHandler handler = new MDRecordHandler();
            
            saxParser.parse(inputStream = DataciteXmlImporterTest.class.getResourceAsStream(
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_1.xml"), handler);
            assertEquals("50|webcrawl____::000015093c397516c0b1b000f38982de", handler.getRecordId());
                        
            // verifying null id
            saxParser.parse(inputStream = DataciteXmlImporterTest.class.getResourceAsStream(
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_3_no_id.xml"), handler);
            assertNull(handler.getRecordId());
            
            // verifying handler reusability
            saxParser.parse(inputStream = DataciteXmlImporterTest.class.getResourceAsStream(
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_2.xml"), handler);
            assertEquals("50|webcrawl____::000038b2e009d799643ba2b05a155877", handler.getRecordId());
            
        } finally {
            if (inputStream!=null) {
                inputStream.close();
            }   
        }
    }

}
