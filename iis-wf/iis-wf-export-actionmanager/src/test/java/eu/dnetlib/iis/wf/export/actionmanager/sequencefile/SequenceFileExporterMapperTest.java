package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.security.InvalidParameterException;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class SequenceFileExporterMapperTest {

    @Mock
    private Context context;
    
    @Captor
    private ArgumentCaptor<Text> keyCaptor;
    
    @Captor
    private ArgumentCaptor<Text> valueCaptor;
    
    
    private SequenceFileExporterMapper mapper = new SequenceFileExporterMapper();

    // ------------------------------------- TESTS -----------------------------------
    
    @Test(expected=InvalidParameterException.class)
    public void testSetupWitoutActionBuilderClassName() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test(expected=RuntimeException.class)
    public void testSetupWithInvalidActionBuilderClassName() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME, "invalid.class.name");
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test(expected=RuntimeException.class)
    public void testSetupWithoutActionSetId() throws Exception {
        // given
        Configuration conf = new Configuration();
        conf.set(EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME, 
                "eu.dnetlib.iis.wf.export.actionmanager.sequencefile.MockDocumentProjectActionBuilderFactory");
        doReturn(conf).when(context).getConfiguration();
        
        // execute
        mapper.setup(context);
    }
    
    @Test
    public void testMap() throws Exception {
        // given
        String actionSetId = "docProjActionSetId";
        Configuration conf = new Configuration();
        conf.set(EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME, 
                "eu.dnetlib.iis.wf.export.actionmanager.sequencefile.MockDocumentProjectActionBuilderFactory");
        conf.set(buildActionSetIdPropertyKey(AlgorithmName.document_referencedProjects), actionSetId);
        doReturn(conf).when(context).getConfiguration();
        mapper.setup(context);

        DocumentToProject docToProj = DocumentToProject.newBuilder()
                .setDocumentId("docId").setProjectId("projId").setConfidenceLevel(0.9f).build();
        
        // execute
        mapper.map(new AvroKey<DocumentToProject>(docToProj), null, context);
        
        // assert
        verify(context, times(1)).write(keyCaptor.capture(), valueCaptor.capture());
        assertTrue(StringUtils.isNotBlank(keyCaptor.getValue().toString()));
        assertEquals(MockDocumentProjectActionBuilderFactory.toStringRepresentation(docToProj), 
                valueCaptor.getValue().toString());
    }

    // ------------------------------------- PRIVATE -----------------------------------
    
    private static String buildActionSetIdPropertyKey(AlgorithmName algorithmName) {
        return EXPORT_ACTION_SETID + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + algorithmName;
    }

}
