package eu.dnetlib.iis.wf.export.actionmanager.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Agent.AGENT_TYPE;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SequenceFileActionManagerServiceFacadeTest {

    @Mock
    private SequenceFile.Writer writer;
    
    @Captor
    private ArgumentCaptor<Writable> keyCaptor;
    
    @Captor
    private ArgumentCaptor<Writable> valueCaptor;
    
    
    private SequenceFileActionManagerServiceFacade facade;
    
    @Before
    public void init() {
        facade = new SequenceFileActionManagerServiceFacade(writer);
    }
    
    @Test
    public void testStoreNullActions() throws Exception {
        // execute
        facade.storeActions(null);
        
        // assert
        verify(writer, never()).append(Mockito.any(), Mockito.any());
    }
    
    @Test
    public void testStoreActions() throws Exception {
        // given
        String rawSet = "rawSet";
        Agent agent = new Agent("agentId", "test-agent", AGENT_TYPE.service);
        String targetRowKey = "rowKey";
        String targetColumnFamily = "colFam";
        String targetColumn = "col";

        Oaf oaf = Oaf.newBuilder().setKind(Kind.entity).build();
        
        AtomicAction action = new AtomicAction(rawSet, agent, targetRowKey, targetColumnFamily, targetColumn, oaf.toByteArray());
        List<AtomicAction> actions = new ArrayList<>();
        actions.add(action);
        
        // execute
        facade.storeActions(actions);
        
        // assert
        verify(writer).append(keyCaptor.capture(), valueCaptor.capture());
        assertEquals(action.getRowKey(), keyCaptor.getValue().toString());
        AtomicAction recreatedAction = AtomicAction.fromJSON(valueCaptor.getValue().toString());
        assertNotNull(recreatedAction);
        assertEquals(action.getRawSet(), recreatedAction.getRawSet());
        assertNotNull(recreatedAction.getAgent());
        assertEquals(agent.getId(), recreatedAction.getAgent().getId());
        assertEquals(agent.getName(), recreatedAction.getAgent().getName());
        assertEquals(agent.getType(), recreatedAction.getAgent().getType());
        assertEquals(action.getTargetColumn(), recreatedAction.getTargetColumn());
        assertEquals(action.getTargetColumnFamily(), recreatedAction.getTargetColumnFamily());
        assertEquals(action.getTargetRowKey(), recreatedAction.getTargetRowKey());
        Oaf recreatedOaf = Oaf.newBuilder().mergeFrom(recreatedAction.getTargetValue()).build();
        assertEquals(oaf.getKind(), recreatedOaf.getKind());
        
    }

    @Test
    public void testClose() throws Exception {
        // execute
        facade.close();
        
        // assert
        verify(writer, Mockito.times(1)).close();
    }
}
