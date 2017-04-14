package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_APPROVED_COLUMNFAMILIES_CSV;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;



/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ImportInformationSpaceMapperTest {

    @Mock
    private Context context;
    
    @Captor
    private ArgumentCaptor<Text> keyCaptor;
    
    @Captor
    private ArgumentCaptor<InfoSpaceRecord> valueCaptor;
    
    
    private ImportInformationSpaceMapper mapper = new ImportInformationSpaceMapper();

    // ------------------------------------- TESTS -----------------------------------
    
    @Test
    public void testMapNotApprovedColFam() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(IMPORT_APPROVED_COLUMNFAMILIES_CSV, "approvedColFam");
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        Text key = new Text(buildKey("rowKey", "notApprovedColFam", "qualifier"));
        Text value = new Text("jsonContent");
        
        // execute
        mapper.map(key, value, context);
        
        // assert
        verify(context, never()).write(any(), any());
    }

    @Test
    public void testMap() throws Exception {
        // given
        Configuration conf = new Configuration();
        String approvedColumnFamily = "approvedColFam";
        conf.set(IMPORT_APPROVED_COLUMNFAMILIES_CSV, approvedColumnFamily);
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        String rowKey = "rowKey";
        String qualifier = "qualifier";
        String valueStr = "oafJsonContent";
        Text key = new Text(buildKey(rowKey, approvedColumnFamily, qualifier));
        Text value = new Text(valueStr);
        
        // execute
        mapper.map(key, value, context);
        
        // assert
        verify(context, times(1)).write(keyCaptor.capture(), valueCaptor.capture());
        Text id = keyCaptor.getValue();
        assertNotNull(id);
        assertEquals(rowKey, id.toString());
        
        InfoSpaceRecord infoSpaceRecord = valueCaptor.getValue();
        assertNotNull(infoSpaceRecord);
        assertEquals(approvedColumnFamily, infoSpaceRecord.getColumnFamily().toString());
        assertEquals(qualifier, infoSpaceRecord.getQualifier().toString());
        assertEquals(value, infoSpaceRecord.getOafJson());
    }
    
    // ------------------------------------- PRIVATE -----------------------------------

    private String buildKey(String rowKey, String columnFamily, String qualifier) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(rowKey);
        strBuilder.append(ImportInformationSpaceMapper.KEY_SEPARATOR);
        strBuilder.append(columnFamily);
        strBuilder.append(ImportInformationSpaceMapper.KEY_SEPARATOR);
        strBuilder.append(qualifier);
        return strBuilder.toString();
    }
}
