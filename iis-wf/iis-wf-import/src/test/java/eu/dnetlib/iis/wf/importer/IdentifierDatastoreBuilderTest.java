package eu.dnetlib.iis.wf.importer;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.Identifier;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static eu.dnetlib.iis.wf.importer.AbstractIdentifierDatastoreBuilder.PORT_OUT_IDENTIFIER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class IdentifierDatastoreBuilderTest {

    private static final String PARAM_NAME_IDENTIFIERS = "identifiers";
    
    private static final String PARAM_NAME_IDENTIFIERS_BLACKLISTED = "identifiers.blacklisted";
    
    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private AbstractIdentifierDatastoreBuilder identifierDatastoreBuilder;
    
    @Mock
    private DataFileWriter<Identifier> identifierWriter;
    
    @Captor
    private ArgumentCaptor<Identifier> identifierCaptor;
    
    private boolean createEmptyDataStoreIndicator;
    
    @BeforeEach
    public void init() {

        
        Map<String, Path> output = new HashMap<>();
        output.put(PORT_OUT_IDENTIFIER, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.createEmptyDataStoreIndicator = false;
        this.portBindings = new PortBindings(Collections.emptyMap(), output);
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        
        this.identifierDatastoreBuilder = new AbstractIdentifierDatastoreBuilder(
                PARAM_NAME_IDENTIFIERS, PARAM_NAME_IDENTIFIERS_BLACKLISTED) {
            
            @Override
            protected DataFileWriter<Identifier> createWriter(FileSystemPath path, 
                    Schema schema, String dataStoreFileName) throws IOException {
                return identifierWriter;
            }
            
            @Override
            protected DataFileWriter<Identifier> createEmptyDataStore(
                    FileSystemPath path, Schema schema) throws IOException {
                createEmptyDataStoreIndicator = true;
                return identifierWriter;
            }
        };
    }
    
    // ----------------------------------- TESTS -----------------------------------
    
    @Test
    public void testGetInputPorts() throws Exception {
        // execute
        Map<String, PortType> result = identifierDatastoreBuilder.getInputPorts();
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testGetOutputPorts() {
        // execute
        Map<String, PortType> result = identifierDatastoreBuilder.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_OUT_IDENTIFIER));
        assertTrue(result.get(PORT_OUT_IDENTIFIER) instanceof AvroPortType);
        assertSame(Identifier.SCHEMA$, ((AvroPortType) result.get(PORT_OUT_IDENTIFIER)).getSchema());
    }

    @Test
    public void testRunWithoutIdentifierParam() {
        // execute
        assertThrows(IllegalArgumentException.class,
                () -> identifierDatastoreBuilder.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunWithBlankIdentifierParam() throws Exception {
        // given
        parameters.put(PARAM_NAME_IDENTIFIERS, "");
        
        // execute
        identifierDatastoreBuilder.run(portBindings, conf, parameters);
        
        // assert
        assertTrue(createEmptyDataStoreIndicator);
        verify(identifierWriter, never()).append(any());
    }
    
    @Test
    public void testRun() throws Exception {
        // given
        String id1 = "id1";
        String id2 = "id2";
        parameters.put(PARAM_NAME_IDENTIFIERS, buildIdentifiers(id1, id2));
        
        // execute
        identifierDatastoreBuilder.run(portBindings, conf, parameters);
        
        // assert
        assertFalse(createEmptyDataStoreIndicator);
        verify(identifierWriter, times(2)).append(identifierCaptor.capture());
        assertEquals(id1, identifierCaptor.getAllValues().get(0).getId());
        assertEquals(id2, identifierCaptor.getAllValues().get(1).getId());
    }
    
    @Test
    public void testRunWithBlacklistedIdentifierSet() throws Exception {
        // given
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";
        String id4 = "id4";
        parameters.put(PARAM_NAME_IDENTIFIERS, buildIdentifiers(id1, id2, id3, id4));
        parameters.put(PARAM_NAME_IDENTIFIERS_BLACKLISTED, buildIdentifiers(id3, id4));
        
        // execute
        identifierDatastoreBuilder.run(portBindings, conf, parameters);
        
        // assert
        assertFalse(createEmptyDataStoreIndicator);
        verify(identifierWriter, times(2)).append(identifierCaptor.capture());
        assertEquals(id1, identifierCaptor.getAllValues().get(0).getId());
        assertEquals(id2, identifierCaptor.getAllValues().get(1).getId());
    }
    
    // ------------------------------- PRIVATE -----------------------------------
    
    private static String buildIdentifiers(String... ids) {
        StringBuilder strBuilder = new StringBuilder();
        for (int i=0; i < ids.length; i++) {
            strBuilder.append(ids[i]);
            if (i < ids.length -1) {
                strBuilder.append(WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);
            }
        }
        return strBuilder.toString();
    }

}
