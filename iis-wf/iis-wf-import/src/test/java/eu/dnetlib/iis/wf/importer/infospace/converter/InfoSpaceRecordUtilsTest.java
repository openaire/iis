package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import eu.dnetlib.iis.wf.importer.infospace.InfoSpaceRecord;
import eu.dnetlib.iis.wf.importer.infospace.QualifiedOafJsonRecord;

public class InfoSpaceRecordUtilsTest {

    @Test
    public void test_mapByColumnFamily() throws Exception {
        // given
        String authorshipColFam = "personResult_authorship_hasAuthor";
        String authorshipQual1 = "authorshipQual1";
        String authorshipQual2 = "authorshipQual2";
        String authorshipQual3 = "authorshipQual3";
        String authorshipOafJson1 = "authorshipOafJson1";
        String authorshipOafJson2 = "authorshipOafJson2";
        String authorshipOafJson3 = "authorshipOafJson3";
        
        String dedupColFam = "resultResult_dedup_isMergedIn";
        String dedupQual = "dedupQual";
        String dedupOafJson = "dedupOafJson";
        
        List<InfoSpaceRecord> records = new ArrayList<>();
        records.add(new InfoSpaceRecord(new Text(authorshipColFam),new Text(authorshipQual1),new Text(authorshipOafJson1)));
        records.add(new InfoSpaceRecord(new Text(authorshipColFam),new Text(authorshipQual2),new Text(authorshipOafJson2)));
        records.add(new InfoSpaceRecord(new Text(dedupColFam),new Text(dedupQual),new Text(dedupOafJson)));
        records.add(new InfoSpaceRecord(new Text(authorshipColFam),new Text(authorshipQual3),new Text(authorshipOafJson3)));
        
        // execute
        Map<String,List<QualifiedOafJsonRecord>> mappedRecords = InfoSpaceRecordUtils.mapByColumnFamily(records);
        
        // assert
        assertNotNull(mappedRecords);
        assertEquals(2, mappedRecords.size());

        assertEquals(3, mappedRecords.get(authorshipColFam).size());
        assertEquals(authorshipQual1, mappedRecords.get(authorshipColFam).get(0).getQualifier());
        assertEquals(authorshipQual2, mappedRecords.get(authorshipColFam).get(1).getQualifier());
        assertEquals(authorshipQual3, mappedRecords.get(authorshipColFam).get(2).getQualifier());
        assertEquals(authorshipOafJson1, mappedRecords.get(authorshipColFam).get(0).getOafJson());
        assertEquals(authorshipOafJson2, mappedRecords.get(authorshipColFam).get(1).getOafJson());
        assertEquals(authorshipOafJson3, mappedRecords.get(authorshipColFam).get(2).getOafJson());
        
        assertEquals(1, mappedRecords.get(dedupColFam).size());
        assertEquals(dedupQual, mappedRecords.get(dedupColFam).get(0).getQualifier());
        assertEquals(dedupOafJson, mappedRecords.get(dedupColFam).get(0).getOafJson());
    }
    
}

