package eu.dnetlib.iis.ingest.html;

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
	
    @Override
    protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentText nlm = key.datum();
        if (nlm.getText()!=null) {
            final DocumentText.Builder output = DocumentText.newBuilder();
            output.setId(nlm.getId());
            try {
//            	skipping newlines
//            	output.setText(Jsoup.parse(nlm.getText().toString()).text());
//            	preserving newlines
            	output.setText(cleanNoMarkup(nlm.getText().toString()));
                context.write(new AvroKey<DocumentText>(output.build()), 
                		NullWritable.get());	
            } catch (Exception e) {
            	log.error("exception thrown when trying to extract text representation "
            			+ "from html document identified with: " + nlm.getId(), e);
            }
        }
    }
    
    private static String cleanNoMarkup(String input) {
        final Document.OutputSettings outputSettings = new Document.OutputSettings().prettyPrint(false);
        String output = Jsoup.clean(input, "", Whitelist.none(), outputSettings);
        return output!=null?output.replace("&nbsp;", ""):null;

    }
}
