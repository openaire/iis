package eu.dnetlib.iis.workflows.export.actionmanager;

import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_QUORUM;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.HQuorumPeer;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * External hbase connection test.
 * @author mhorst
 *
 */
public class ExternalHbaseConnectionTestProcess implements Process {

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

		Configuration hbaseConfiguration = HBaseConfiguration.create(conf);
//		Configuration hbaseConfiguration = conf;
		
//		this one worked properly!
//		Configuration hbaseConfiguration = new Configuration();
		
//		setting remote zookeeper address for hbase if any set
		String zookeeperQuorum = ProcessUtils.getParameterValue(
				EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_QUORUM, 
				conf, parameters);
		if (zookeeperQuorum!=null && !zookeeperQuorum.trim().isEmpty() &&
						!zookeeperQuorum.trim().equals(
										WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
			hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum.trim());
//			hbaseConfiguration.set("ha.zookeeper.quorum", zookeeperQuorum.trim());
			System.out.println("zookeeper quorum set to: " + zookeeperQuorum.trim());
			String zookeeperClientPort = ProcessUtils.getParameterValue(
					EXPORT_ACTION_HBASE_REMOTE_ZOOKEEPER_CLIENTPORT, 
					conf, parameters);
			if (zookeeperClientPort!=null && !zookeeperClientPort.trim().isEmpty() &&
							!zookeeperClientPort.trim().equals(
											WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
				hbaseConfiguration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort.trim());
				System.out.println("zookeeper client port set to: " + zookeeperClientPort.trim());
			}
		}
//		setting optionally hbase master to explicit value:
		String hbaseMaster = ProcessUtils.getParameterValue(
				"export.action.hbase.remote.hbase.master", 
				conf, parameters);
		if (hbaseMaster!=null && !hbaseMaster.trim().isEmpty() &&
				!hbaseMaster.trim().equals(
								WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE)) {
			hbaseConfiguration.set("hbase.master", hbaseMaster.trim());
			System.out.println("hbase master set to: " + hbaseMaster.trim());
		}
		
		try {
			System.out.println("listing properties...");
			Iterator<Entry<String,String>> it = hbaseConfiguration.iterator();
			while (it.hasNext()) {
				Entry<String,String> current = it.next();
				System.out.println(current.getKey() + " = " + current.getValue());
			}
			
			System.out.println("checking if HBase is running...");
			HBaseAdmin.checkHBaseAvailable(hbaseConfiguration);
			System.out.println("HBase is running!");
			
			System.out.println("listing tables:");
			HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
			try {
				for (HTableDescriptor tableDesc : admin.listTables()) {
					System.out.println("* " + tableDesc.getNameAsString());
				}	
			} finally {
				admin.close();	
			}
			
//			TODO test static method generating quorum string
			
			ClassLoader cl = HQuorumPeer.class.getClassLoader();
		    final InputStream inputStream =
		      cl.getResourceAsStream(HConstants.ZOOKEEPER_CONFIG_NAME);
		    if (inputStream != null) {
		    	System.out.println("jest zoo.cfg");
		    	Properties properties = new Properties();
		    	properties.load(inputStream);
		    	System.out.println("listing zoo.cfg props");
		    	properties.list(System.out);
//		    	To jest powód problemu: całość jest wczytywana z pliku zoo.cfg i nie można overridować
		    	
		    } else {
		    	System.out.println("brak zoo.cfg");
		    }
			
			Properties zkProps = ZKConfig.makeZKProps(hbaseConfiguration);
			System.out.println("listing zk props");
//			już tutaj się psuje! prawdopodobnie przez to, że zoo.cfg jest skonfigurowany!
			zkProps.list(System.out);
			
			String quorumString = ZKConfig.getZKQuorumServersString(hbaseConfiguration);
//			this string is invalid: inspect method
			System.out.println("got quorum string: " + quorumString);
			
			

		} catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			e.printStackTrace();
		} catch (Exception ce) {
			ce.printStackTrace();
		}
	}

}
