package eu.dnetlib.iis.workflows.importer.hbasedump;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.googlecode.protobuf.format.JsonFormat;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Hbase dump importer mapper. Reads records from sequence file and writes them down to hbase.
 * @author mhorst
 *
 */
public class ImportInformationSpaceMapper 
	extends Mapper<Text, Text, NullWritable, NullWritable> {

	private static final String OUTPUT_TABLE_NAME = "output.table.name";
	
	private static final char KEY_SEPARATOR = '@';

	private HTableInterface hTable;
	
	private List<Put> putCache = new ArrayList<Put>();
	
	private int cacheThreshold = 1000;
	
	@Override
	protected void setup(
			Mapper<Text, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String tableName = context.getConfiguration().get(
				OUTPUT_TABLE_NAME);
		if (tableName==null) {
			throw new IOException("hbase table name was not provided!");
		}
		hTable = new HTable(HBaseConfiguration.create(
				context.getConfiguration()), tableName);
	}
	
	@Override
	protected void map(Text key, Text value,
			Mapper<Text, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
//		rowkey@columnFamily@qualifier
		String oafJson = value.toString();
		if (!oafJson.isEmpty()) {
			String[] split = StringUtils.split(key.toString(), KEY_SEPARATOR);
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			JsonFormat.merge(oafJson, oafBuilder);
			Put put = new Put(Bytes.toBytes(split[0]));
			put.add(Bytes.toBytes(split[1]), 
					Bytes.toBytes(split[2]), 
					oafBuilder.build().toByteArray());
			putCache.add(put);
			if (putCache.size()>cacheThreshold) {
//				FIXME testing without puts, changed also in cleanup()
//				hTable.put(putCache);
				putCache.clear();
			}
		}	
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
//		FIXME testing without puts
//		if (hTable!=null) {
//			try {
//				if (!putCache.isEmpty()) {
//					hTable.put(putCache);
//					putCache.clear();
//				}	
//			} finally {
//				hTable.flushCommits();
//				hTable.close();
//			}
//		}
	}
	
}
