package eu.dnetlib.iis.importer;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * Simple hbase reader testing hbase connection from oozie workflow.
 * Prints results to standard output.
 * @author mhorst
 *
 */
public class HBaseStdoutPrinter implements eu.dnetlib.iis.core.java.Process {

	/**
	 * Encoding to be used when building identifiers from byte[].
	 */
	protected String encoding = "utf-8"; 
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return new HashMap<String, PortType>();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		String tableName = parameters.get("import.hbase.table.name");
		
		final Configuration localConf = HBaseConfiguration.create(conf);
//		printing out all conf parameters
		System.out.println("listing hbase configuration parameters");
		for (Entry<String, String> entry : localConf) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
		System.out.println("listing hbase entries");
		HTable hTable = new HTable(localConf, tableName);
		try {
			Scan scan = new Scan();
			ResultScanner resultScanner = hTable.getScanner(scan);
			Result result;
			int idx = 1;
			while ((result = resultScanner.next())!=null) {
				System.out.println(idx + " row: " + prepareKeyValueCSV(result.list()));
				idx++;
			}	
		} finally {
			hTable.close();
		}
	}

	protected String prepareKeyValueCSV(List<KeyValue> kvList) throws UnsupportedEncodingException {
		if (kvList!=null) {
			StringBuffer strBuff = new StringBuffer();
			boolean isFirst = true;
			for (KeyValue kv : kvList) {
				if (isFirst) {
					isFirst=false;
				} else {
					strBuff.append(" ,");
				}
				strBuff.append(new String(kv.getKey(), encoding) + "=" + 
						new String(kv.getValue(), encoding));
			}
			return strBuff.toString();
		} else {
			return null;
		}
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
}
