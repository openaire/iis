package eu.dnetlib.iis.wf.ingest.html;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;


/**
 * Module ingesting plain text from HTML document.
 * @author mhorst
 */
public class HtmlToPlaintextIngester extends Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<DocumentText>, NullWritable> {

	private final Logger log = Logger.getLogger(this.getClass());
	
    private final static Document.OutputSettings outputSettings = new Document.OutputSettings().prettyPrint(false);
	
    @Override
    protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentText htmlText = key.datum();
        if (htmlText.getText()!=null) {
            final DocumentText.Builder output = DocumentText.newBuilder();
            output.setId(htmlText.getId());
            try {
//            	preserving newlines
            	output.setText(cleanNoMarkup(htmlText.getText().toString()));
                context.write(new AvroKey<DocumentText>(output.build()), 
                		NullWritable.get());	
            } catch (Exception e) {
            	log.error("exception thrown when trying to extract text representation "
            			+ "from html document identified with: " + htmlText.getId(), e);
            }
        }
    }
    
    private static String cleanNoMarkup(String input) {
        return Jsoup.clean(input, "", Whitelist.none(), outputSettings).replace("&nbsp;", "");
    }
}
