package eu.dnetlib.iis.workflows.export.actionmanager.api;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;

/**
 * Sequence file based action manager service facade. 
 * 
 * @author mhorst
 *
 */
public class SequenceFileActionManagerServiceFacade implements ActionManagerServiceFacade {

	
	private final SequenceFile.Writer writer;

	/**
	 * Default constructor.
	 * 
	 * @param hadoopConf hadoop configuration
	 * @param outputDirRoot root output directory
	 * @param outputDirName output subdirectory name
	 */
	public SequenceFileActionManagerServiceFacade(Configuration hadoopConf, 
			String outputDirRoot, String outputDirName) throws IOException {
		this.writer = SequenceFile.createWriter(hadoopConf, 
				Writer.file(new Path(new Path(outputDirRoot, outputDirName),"part-m-00000")),
				Writer.keyClass(Text.class), 
				Writer.valueClass(Text.class), 
				Writer.compression(CompressionType.BLOCK));
	}

	@Override
	public void storeAction(Collection<AtomicAction> actions, Provenance provenance, String trust, String nsprefix)
			throws ActionManagerException {
		if (actions != null) {
			for (AtomicAction action : actions) {
				try {
					Text keyOut = new Text();
					Text valueOut = new Text();
					keyOut.set(action.getRowKey());
					valueOut.set(action.toString());
					writer.append(keyOut, valueOut);
				} catch (IOException e) {
					throw new ActionManagerException(
							"exception occurred when writing action: " + action.toString(), e);
				}
			}
		}
	}

	@Override
	public void close() throws ActionManagerException {
		IOUtils.closeStream(writer);
	}

}
