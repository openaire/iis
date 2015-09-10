package eu.dnetlib.iis.ingest.webcrawl.fundings;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;


/**
 * Module ingesting fundings details from webcrawl XML documents.
 * @author mhorst
 */
public class WebcrawlFundingsIngester extends Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<DocumentText>, NullWritable> {

    @Override
    protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentText nlm = key.datum();
        if (nlm.getText()!=null) {
            final DocumentText.Builder output = DocumentText.newBuilder();
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
    			
    			WebcrawlFundingsHandler xmlHandler = new WebcrawlFundingsHandler();
				saxParser.parse(new InputSource(
						new StringReader(nlm.getText().toString())), 
						xmlHandler);
				output.setText(xmlHandler.getFundingText());
                context.write(new AvroKey<DocumentText>(output.build()), 
                		NullWritable.get());
        	 } catch (ParserConfigurationException e) {
             	throw new IOException("Fundings text extraction failed for id " + nlm.getId() + 
             			" and text: " + nlm.getText(), e);
        	 } catch (SAXException e) {
 				throw new IOException("Fundings text extraction failed for id " + nlm.getId() + 
             			" and text: " + nlm.getText(), e);
        	 }
        }
    }
}
