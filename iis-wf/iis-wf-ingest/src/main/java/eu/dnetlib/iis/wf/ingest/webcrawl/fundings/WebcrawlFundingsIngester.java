package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;

/**
 * Module ingesting fundings details from webcrawl XML documents.
 * 
 * @author mhorst
 */
public class WebcrawlFundingsIngester
		extends Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<DocumentText>, NullWritable> {

	private final Logger log = Logger.getLogger(this.getClass());

	@Override
	protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		DocumentText xmlText = key.datum();
		if (!StringUtils.isBlank(xmlText.getText())) {
			try {
				// disabling validation
				SAXParserFactory saxFactory = SAXParserFactory.newInstance();
				saxFactory.setValidating(false);
				SAXParser saxParser = saxFactory.newSAXParser();
				XMLReader reader = saxParser.getXMLReader();
				reader.setFeature("http://xml.org/sax/features/validation", false);
				reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
				reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

				WebcrawlFundingsHandler xmlHandler = new WebcrawlFundingsHandler();
				saxParser.parse(new InputSource(new StringReader(xmlText.getText().toString())), xmlHandler);
				if (!StringUtils.isBlank(xmlHandler.getFundingText())) {
					DocumentText.Builder output = DocumentText.newBuilder();
					output.setId(xmlText.getId());
					output.setText(xmlHandler.getFundingText());
					context.write(new AvroKey<DocumentText>(output.build()), NullWritable.get());
				}
			} catch (Exception e) {
				log.error("Fundings text extraction failed for id " + xmlText.getId() + 
						" and text: " + xmlText.getText(), e);
			}
		}
	}
}
