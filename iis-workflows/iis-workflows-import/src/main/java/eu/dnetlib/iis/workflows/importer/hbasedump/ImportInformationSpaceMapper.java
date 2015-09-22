package eu.dnetlib.iis.workflows.importer.hbasedump;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
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
	extends Mapper<Text, Text, ImmutableBytesWritable, Put> {

	private static final char KEY_SEPARATOR = '@';
	
	private ImmutableBytesWritable ibw;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		ibw = new ImmutableBytesWritable();
	}
	
	@Override
	protected void map(Text key, Text value,
			Mapper<Text, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String oafJson = value.toString();
		if (StringUtils.isNotBlank(oafJson)) {
			String[] split = StringUtils.split(key.toString(), KEY_SEPARATOR);
			if (split.length!=3) {
				throw new IOException("invalid key, "
						+ "expected 'rowkey@columnFamily@qualifier', got: " + key);
			}
			byte[] rowKey = Bytes.toBytes(split[0]);
			byte[] columnFamily = Bytes.toBytes(split[1]);
			byte[] qualifier = Bytes.toBytes(split[2]);
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			JsonFormat.merge(oafJson, oafBuilder);
			Put put = new Put(rowKey);
			put.add(columnFamily, qualifier, 
					oafBuilder.build().toByteArray());
//			skipping writing to WAL to speed up importing process
			put.setWriteToWAL(false);
			ibw.set(rowKey);
			context.write(ibw, put);
		}	
	}
	
}
