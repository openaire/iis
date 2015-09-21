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
//		rowkey@columnFamily@qualifier
		String oafJson = value.toString();
		if (StringUtils.isNotBlank(oafJson)) {
			String[] split = StringUtils.split(key.toString(), KEY_SEPARATOR);
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			JsonFormat.merge(oafJson, oafBuilder);
			byte[] rowKey = Bytes.toBytes(split[0]);
			Put put = new Put(rowKey);
			put.add(Bytes.toBytes(split[1]), 
					Bytes.toBytes(split[2]), 
					oafBuilder.build().toByteArray());
//			skipping writing to WAL to speed up importing process
			put.setWriteToWAL(false);
			ibw.set(rowKey);
			context.write(ibw, put);
		}	
	}
	
}
