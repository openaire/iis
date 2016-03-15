package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;

/**
 * {@link DocumentContentUrl} mime type based dispatcher writing {@link DocumentContentUrl} objects to dedicated output port.
 * @author mhorst
 *
 */
public class DocumentContentUrlDispatcher extends Mapper<AvroKey<DocumentContentUrl>, NullWritable, NullWritable, NullWritable> {

	private final static Logger log = Logger.getLogger(DocumentContentUrlDispatcher.class);
	
	private static final String PROPERTY_PREFIX_MIMETYPES_CSV = "mimetypes.csv.";
	
	private static final String PROPERTY_MULTIPLEOUTPUTS = "avro.mapreduce.multipleoutputs";
	
	/**
	 * Mime type to port name mappings.
	 */
	private Map<CharSequence,String> mimeTypeToPortNameMap;
	
	private MultipleOutputs mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.mos = new MultipleOutputs(context);
		this.mimeTypeToPortNameMap = new HashMap<CharSequence, String>();
//		iterating through output port names and looking for mimetypes properties defined for each output port
		String[] portNames = StringUtils.split(
				context.getConfiguration().get(PROPERTY_MULTIPLEOUTPUTS));
		for (String portName : portNames) {
			String currentMimeTypePropName = PROPERTY_PREFIX_MIMETYPES_CSV + portName; 
			if (context.getConfiguration().get(
					currentMimeTypePropName)!=null) {
				String[] currentPortMimeTypes = StringUtils.split(
						context.getConfiguration().get(currentMimeTypePropName),
						WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);	
				for (String currentPortMimeType : currentPortMimeTypes) {
					if (!currentPortMimeType.isEmpty() && 
							!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
							currentPortMimeType)) {
						this.mimeTypeToPortNameMap.put(currentPortMimeType.toLowerCase(), portName);	
					}
				}
			} else {
				log.warn("undefined property '" + currentMimeTypePropName + 
						"', no data will be dispatched to port '" + portName + "'");
			}
		}
	}

	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		mos.close();
	}
	
	@Override
	public void map(AvroKey<DocumentContentUrl> key, NullWritable ignore, Context context)
			throws IOException, InterruptedException {
		DocumentContentUrl currentRecord = key.datum();
		if (currentRecord.getUrl()!=null) {
			if (currentRecord.getMimeType()!=null) {
				String lowercasedMimeType = currentRecord.getMimeType().toString().toLowerCase();
				if (this.mimeTypeToPortNameMap.containsKey(lowercasedMimeType)) {
					mos.write(this.mimeTypeToPortNameMap.get(lowercasedMimeType), 
							new AvroKey<DocumentContentUrl>(key.datum()));
				} else {
					log.warn("skipping, got unhandled mime type: " + 
							lowercasedMimeType + " for object: " + currentRecord.getId());
				}	
			} else {
				log.warn("got null mime type for object: " + currentRecord.getId());
			}		
		}
	}

}
