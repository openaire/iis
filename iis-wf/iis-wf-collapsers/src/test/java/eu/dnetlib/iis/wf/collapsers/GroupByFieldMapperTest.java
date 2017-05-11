package eu.dnetlib.iis.wf.collapsers;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.schemas.Identifier;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByFieldMapperTest {

    @Mock
    private Context context;
    
    private GroupByFieldMapper mapper = new GroupByFieldMapper();

    
    // ------------------------------------- TESTS -----------------------------------
    
    @Test
    public void testGroupingWithoutBlockingField() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        String idValue = "someId";
        Identifier id = Identifier.newBuilder().setId(idValue).build();
        
        // execute
        mapper.map(new AvroKey<>(id), null, context);
        
        // validate
        verify(context, times(1)).write(new AvroKey<String>(null), new AvroValue<Identifier>(id));
    }
    
    @Test
    public void testGroupingWithInvalidBlockingField() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(GroupByFieldMapper.BLOCKING_FIELD, "invalid");
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        String idValue = "someId";
        Identifier id = Identifier.newBuilder().setId(idValue).build();
        
        // execute
        mapper.map(new AvroKey<>(id), null, context);
        
        // validate
        verify(context, times(1)).write(new AvroKey<String>(null), new AvroValue<Identifier>(id));
    }

    @Test
    public void testGrouping() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(GroupByFieldMapper.BLOCKING_FIELD, "id");
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);
        String idValue = "someId";
        Identifier id = Identifier.newBuilder().setId(idValue).build();
        
        // execute
        mapper.map(new AvroKey<>(id), null, context);
        
        // validate
        verify(context, times(1)).write(new AvroKey<String>(idValue), new AvroValue<Identifier>(id));
    }
}
