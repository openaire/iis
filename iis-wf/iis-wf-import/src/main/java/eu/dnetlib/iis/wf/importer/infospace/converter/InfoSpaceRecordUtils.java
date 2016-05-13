package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.wf.importer.infospace.InfoSpaceRecord;
import eu.dnetlib.iis.wf.importer.infospace.QualifiedOafJsonRecord;

/**
 * {@link InfoSpaceRecord}e related utility class.
 * @author mhorst
 *
 */
public class InfoSpaceRecordUtils {

    private InfoSpaceRecordUtils(){}
    
    /**
     * Maps {@link InfoSpaceRecord}s by column family.
     * @param records records to be mapped into columnFamily->record
     */
    public static Map<String, List<QualifiedOafJsonRecord>> mapByColumnFamily(Iterable<InfoSpaceRecord> records) {
        Preconditions.checkNotNull(records);
        Map<String, List<QualifiedOafJsonRecord>> oafRecordsByFamilyMap = Maps.newHashMap();
        for (InfoSpaceRecord record : records) {
            String columnFamily = record.getColumnFamily().toString();
            List<QualifiedOafJsonRecord> storedRecords = oafRecordsByFamilyMap.get(columnFamily);
            if (storedRecords==null) {
                oafRecordsByFamilyMap.put(columnFamily, storedRecords = Lists.newArrayList());
            }
            storedRecords.add(new QualifiedOafJsonRecord(record.getQualifier().toString(), record.getOafJson().toString()));
        }
        return oafRecordsByFamilyMap;
    }
}
