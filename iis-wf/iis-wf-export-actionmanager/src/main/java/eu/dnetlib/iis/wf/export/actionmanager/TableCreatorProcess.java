package eu.dnetlib.iis.wf.export.actionmanager;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_TABLE_INITIALIZE;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_TABLE_NAME;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import eu.dnetlib.actionmanager.ActionManagerConstants;
import eu.dnetlib.actionmanager.ActionManagerConstants.COLUMN_FAMILIES;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.wf.export.actionmanager.api.HBaseActionManagerServiceFacade;

/**
 * Table creator process.
 * @author mhorst
 *
 */
public class TableCreatorProcess implements Process {

	private final Logger log = Logger.getLogger(this.getClass());
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		String initializeTable = ProcessUtils.getParameterValue(
				EXPORT_ACTION_HBASE_TABLE_INITIALIZE, 
				conf, parameters);
		if (initializeTable!=null && !initializeTable.isEmpty() && 
				Boolean.parseBoolean(initializeTable)) {
			String hBaseTableName = ProcessUtils.getParameterValue(
					EXPORT_ACTION_HBASE_TABLE_NAME, 
					conf, parameters);
			if (hBaseTableName!=null) {
				prepareTable(false, hBaseTableName, 
						HBaseActionManagerServiceFacade.buildHBaseConfiguration(
								conf, parameters));
			} else {
				throw new RuntimeException("no action manager hbase table name provided!");
			}
		} else {
			log.warn("skipping table initialization");
		}		
	}
	
	/**
	 * Prepares HBase action table. 
	 * This method originates from dnet-actionmanager-service:
	 * eu.dnetlib.actionmanager.hbase.HBaseClient#prepareTable
	 * @param delete
	 * @param tableName
	 * @param config
	 * @throws IOException
	 */
	private void prepareTable(boolean delete, String tableName,
			Configuration config) throws IOException {
		final HBaseAdmin admin = new HBaseAdmin(config);
		try {
			if (delete && admin.tableExists(tableName)) {
				log.info("Deleting existing hbase table: " + tableName);
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			if (!admin.tableExists(tableName)) {
				log.info("Creating missing hbase table: " + tableName);
				admin.createTable(new HTableDescriptor(tableName));
			}
			final HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(tableName));

			Set<String> currents = Sets.newHashSet();
			for (HColumnDescriptor hcd : desc.getColumnFamilies()) {
				currents.add(hcd.getNameAsString());
			}
			Set<String> missing = Sets.newHashSet();
			for (COLUMN_FAMILIES cf : ActionManagerConstants.COLUMN_FAMILIES.values()) {
				if (!currents.contains(cf.toString())) {
					missing.add(cf.toString());
				}
			}
			if (!missing.isEmpty()) {
				if (admin.isTableEnabled(tableName)) {
					admin.disableTable(tableName);
				}
				for (String column : missing) {
					log.info("hbase table: '" + tableName + "', adding columnFamily: " + column);
					admin.addColumn(tableName, new HColumnDescriptor(column));
				}
				admin.enableTable(tableName);
			}
		} finally {
			admin.close();
		}
	}
	
}
