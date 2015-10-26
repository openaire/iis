package eu.dnetlib.iis.workflows.export.actionmanager.api;

import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_QUORUM;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.core.java.ProcessUtils;

/**
 * HBase-backed action manager service facade.
 * This implementation is not thread-safe.
 * @author mhorst
 *
 */
public class HBaseActionManagerServiceFacade implements
		ActionManagerServiceFacade {
	
	private final HTableInterface hTable;

	private List<Put> putCache = new ArrayList<Put>();
	
	private int cacheThreshold = 1000;
	
	/**
	 * Default constructor taking context parameters.
	 * @param context
	 */
	public HBaseActionManagerServiceFacade(Configuration config) throws IOException {
		this(config, Collections.<String, String>emptyMap());
	}
	
	/**
	 * Default constructor taking context parameters.
	 * @param config
	 * @param parameters
	 */
	public HBaseActionManagerServiceFacade(Configuration hadoopConf,
			Map<String, String> parameters) throws IOException {
		String hBaseTableName = ProcessUtils.getParameterValue(
				EXPORT_ACTION_HBASE_TABLE_NAME, 
				hadoopConf, parameters);
		if (hBaseTableName!=null) {
			hTable = new HTable(
					buildHBaseConfiguration(hadoopConf, parameters), 
					hBaseTableName);
		} else {
			throw new RuntimeException("no action manager hbase table name provided!");
		}
	}

	/**
	 * Creates hbase configuration.
	 * @param hadoopConf
	 * @param parameters
	 * @return hbase configuration
	 */
	public static Configuration buildHBaseConfiguration(
			Configuration hadoopConf,
			Map<String, String> parameters) {
		Configuration hbaseConfiguration = HBaseConfiguration.create(hadoopConf);
//		setting remote zookeeper address for hbase if any set
		String zookeeperQuorum = ProcessUtils.getParameterValue(
				EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_QUORUM, 
				hadoopConf, parameters);
		if (zookeeperQuorum!=null && !zookeeperQuorum.trim().isEmpty() &&
						!zookeeperQuorum.trim().equals(
										WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
			hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum.trim());
			String zookeeperClientPort = ProcessUtils.getParameterValue(
					EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT, 
					hadoopConf, parameters);
			if (zookeeperClientPort!=null && !zookeeperClientPort.trim().isEmpty() &&
							!zookeeperClientPort.trim().equals(
											WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
				hbaseConfiguration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort.trim());
			}
		}
		return hbaseConfiguration;
	}
	
	@Override
	public void storeAction(
			Collection<AtomicAction> actions, Provenance provenance,
			String trust, String nsprefix) throws ActionManagerException {
		if (actions!=null) {
			for (AtomicAction action : actions) {
				putCache.addAll(action.asPutOperations(
						null, provenance, trust, nsprefix));	
			}
			if (putCache.size()>cacheThreshold) {
				try {
					hTable.put(putCache);
					putCache.clear();
				} catch (IOException e) {
					throw new ActionManagerException(e);
				}
			}
		}
	}
	
	@Override
	public void close() throws ActionManagerException {
		if (hTable!=null) {
			try {
				if (!putCache.isEmpty()) {
					try {
						hTable.put(putCache);
						putCache.clear();
					} catch (IOException e) {
						throw new ActionManagerException(e);
					}
				}	
			} finally {
				try {
					hTable.flushCommits();
					hTable.close();
				} catch (IOException e) {
					throw new ActionManagerException(e);
				}
			}
		}
	}

	public void setCacheThreshold(int cacheThreshold) {
		this.cacheThreshold = cacheThreshold;
	}

}
