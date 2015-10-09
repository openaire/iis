package eu.dnetlib.iis.workflows.ingest.pmc.metadata;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;


/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 * @author mhorst
 */
public class MetadataImporter extends Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<ExtractedDocumentMetadata>, NullWritable> {

    @Override
    protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentText nlm = key.datum();
        if (!StringUtils.isBlank(nlm.getText())) {
            final ExtractedDocumentMetadata.Builder output = ExtractedDocumentMetadata.newBuilder();
            output.setId(nlm.getId());
            try {
//            	disabling validation
    			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
    			saxFactory.setValidating(false);
    			SAXParser saxParser = saxFactory.newSAXParser();
    			XMLReader reader = saxParser.getXMLReader();
    			reader.setFeature("http://xml.org/sax/features/validation", false);
    			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
    			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    			
    			PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(output);
				saxParser.parse(new InputSource(
						new StringReader(nlm.getText().toString())), 
						pmcXmlHandler);
                context.write(new AvroKey<ExtractedDocumentMetadata>(output.build()), 
                		NullWritable.get());
        	 } catch (ParserConfigurationException e) {
             	throw new IOException("Text extraction failed for id " + nlm.getId() + 
             			" and text: " + nlm.getText(), e);
        	 } catch (SAXException e) {
 				throw new IOException("Text extraction failed for id " + nlm.getId() + 
             			" and text: " + nlm.getText(), e);
        	 }
        }
    }
}
