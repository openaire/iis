package eu.dnetlib.iis.importer.javamapreduce.hack;

import static eu.dnetlib.iis.importer.ImportWorkflowRuntimeParameters.IMPORT_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT;
import static eu.dnetlib.iis.importer.ImportWorkflowRuntimeParameters.IMPORT_HBASE_REMOTE_ZOOKEEPER_QUORUM;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.core.java.ProcessUtils;
import eu.dnetlib.iis.core.javamapreduce.hack.SchemaSetter;

/**
 * Class to be used in Oozie map-reduce workflow node definition.
 * @author mhorst
 *
 */
public class TableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {

	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		SchemaSetter.set(context.getConfiguration());
		return super.createRecordReader(split, context);
	}

	@Override
	public void setConf(Configuration sourceConfig) {
//		overriding this method in order to load hbase-site.xml environment properties
//		which apparently is not done by default. 
//		This was introduced after removing zoo.cfg from classpath, otherwise zookeeper properties will have to be provided at runtime
		Configuration hbaseConf = HBaseConfiguration.create(sourceConfig);
//		overriding zookeeper properties when provided and not set to $UNDEFINED$ value
		String zookeeperQuorum = ProcessUtils.getParameterValue(
				IMPORT_HBASE_REMOTE_ZOOKEEPER_QUORUM, 
				sourceConfig, null);
		if (zookeeperQuorum!=null && !zookeeperQuorum.trim().isEmpty() &&
						!zookeeperQuorum.trim().equals(
										WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
			hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum.trim());
			String zookeeperClientPort = ProcessUtils.getParameterValue(
					IMPORT_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT, 
					sourceConfig, null);
			if (zookeeperClientPort!=null && !zookeeperClientPort.trim().isEmpty() &&
							!zookeeperClientPort.trim().equals(
											WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
				hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort.trim());
			}
		}
		super.setConf(hbaseConf);
	}
	
}
