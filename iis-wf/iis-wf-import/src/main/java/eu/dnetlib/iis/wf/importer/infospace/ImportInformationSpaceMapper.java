package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_APPROVED_COLUMNFAMILIES_CSV;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * Hbase dump importer mapper. Reads records from sequence file and groups them by row identifier.
 * 
 * @author mhorst
 *
 */
public class ImportInformationSpaceMapper extends Mapper<Text, Text, Text, InfoSpaceRecord> {

    private static final char KEY_SEPARATOR = '@';

    private Text id;

    private Set<String> approvedColumnFamilies = Collections.emptySet();

    // ------------------------ LOGIC --------------------------

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        id = new Text();
        String approvedColumnFamiliesCSV = WorkflowRuntimeParameters.getParamValue(IMPORT_APPROVED_COLUMNFAMILIES_CSV, context);
        if (StringUtils.isNotBlank(approvedColumnFamiliesCSV)) {
            approvedColumnFamilies = Sets.newHashSet(Splitter.on(DEFAULT_CSV_DELIMITER).trimResults().split(approvedColumnFamiliesCSV));
        }
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, InfoSpaceRecord>.Context context)
                    throws IOException, InterruptedException {
        String oafJson = value.toString();
        if (StringUtils.isNotBlank(oafJson)) {
            String[] split = StringUtils.split(key.toString(), KEY_SEPARATOR);
            if (split.length != 3) {
                throw new IOException("invalid key, " + "expected 'rowkey" + KEY_SEPARATOR + "columnFamily"
                        + KEY_SEPARATOR + "qualifier', got: " + key);
            }
            if (approvedColumnFamilies.contains(split[1])) {
                id.set(split[0]);
                context.write(id, new InfoSpaceRecord(new Text(split[1]), new Text(split[2]), new Text(oafJson)));
            }
        }
    }
}
