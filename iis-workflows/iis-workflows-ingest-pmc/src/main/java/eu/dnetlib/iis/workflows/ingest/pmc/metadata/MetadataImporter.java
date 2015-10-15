package eu.dnetlib.iis.workflows.ingest.pmc.metadata;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.core.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;


/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 * @author mhorst
 */
public class MetadataImporter extends Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable> {

	protected final Logger log = Logger.getLogger(this.getClass());
	
	public static final String FAULT_TEXT = "text";
	
	/**
	 * Multiple outputs.
	 */
	protected MultipleOutputs mos = null;
	
	/**
	 * Document metadata named output.
	 */
	protected String namedOutputMeta;

	/**
	 * Fault named output.
	 */
	protected String namedOutputFault;
	
    @Override
	protected void setup(Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
    	namedOutputMeta = context.getConfiguration().get("output.meta");
		if (namedOutputMeta==null || namedOutputMeta.isEmpty()) {
			throw new RuntimeException("no named output provided for metadata");
		}
		namedOutputFault = context.getConfiguration().get("output.fault");
		if (namedOutputFault==null || namedOutputFault.isEmpty()) {
			throw new RuntimeException("no named output provided for fault");
		}
    	mos = new MultipleOutputs(context);
	}

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
				mos.write(namedOutputMeta, 
						new AvroKey<ExtractedDocumentMetadata>(output.build()));
        	 } catch (ParserConfigurationException e) {
        		 Map<CharSequence, CharSequence> auditSupplementaryData = new HashMap<CharSequence, CharSequence>();
        			auditSupplementaryData.put(FAULT_TEXT, nlm.getText());
        		 mos.write(namedOutputFault, 
        				 new AvroKey<Fault>(FaultUtils.exceptionToFault(nlm.getId(), e, auditSupplementaryData)));
        	 } catch (SAXException e) {
				Map<CharSequence, CharSequence> auditSupplementaryData = new HashMap<CharSequence, CharSequence>();
				auditSupplementaryData.put(FAULT_TEXT, nlm.getText());
				mos.write(namedOutputFault,
						new AvroKey<Fault>(FaultUtils.exceptionToFault(nlm.getId(), e, auditSupplementaryData)));
        	 }
        }
    }
    
    /* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
		log.debug("cleanup: closing multiple outputs...");
        mos.close();
        log.debug("cleanup: multiple outputs closed");
    }
}
