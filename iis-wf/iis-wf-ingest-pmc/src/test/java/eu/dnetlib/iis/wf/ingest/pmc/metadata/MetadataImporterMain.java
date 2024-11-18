package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;

/**
 * This class is responsible for performing metadata extraction out of the JATS XML file which location is provided as input parameter.
 * @author mhorst
 */
public class MetadataImporterMain {
    
    private static final String encoding = "utf-8"; 
    
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
        if (args.length == 1) {
            if (StringUtils.isBlank(args[0])) {
                throw new RuntimeException("No valid location provided!");
            }
            
            ExtractedDocumentMetadata.Builder builder  = ExtractedDocumentMetadata.newBuilder();
            builder.setId("");
            builder.setText("");
            extractMetadata(getFileContent(args[0]), builder);
            
            DatumWriter<ExtractedDocumentMetadata> writer = new SpecificDatumWriter<>(ExtractedDocumentMetadata.class);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(ExtractedDocumentMetadata.getClassSchema(), stream);
            writer.write(builder.build(), jsonEncoder);
            jsonEncoder.flush();
            System.out.println(new String(stream.toByteArray(), encoding));
            
        } else {
            throw new RuntimeException("invalid number of input arguments: " + args.length 
                    + ", expecting a single argument with JATS XML file location!");
        }
    }
    
    /**
     * Extracts metadata from given xml input by supplementing metada in output builder.
     * 
     * @param xmlInput JATS XML file content
     * @param output extracted metadata
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    protected static void extractMetadata(byte[] xmlInput, ExtractedDocumentMetadata.Builder output)
            throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        saxFactory.setValidating(false);
        SAXParser saxParser = saxFactory.newSAXParser();
        XMLReader reader = saxParser.getXMLReader();
        reader.setFeature("http://xml.org/sax/features/validation", false);
        reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        JatsXmlHandler pmcXmlHandler = new JatsXmlHandler(output);
        saxParser.parse(new InputSource(new ByteArrayInputStream(xmlInput)), pmcXmlHandler);
    }
    
    /**
     * Reads file content from the location provided as input parameter.
     */
    private static byte[] getFileContent(String location) throws IOException {
        return Files.readAllBytes(Paths.get(location));
    }
}
