package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.PortBindings;

/**
 * {@link RdbDataCleaner} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class RdbDataCleanerTest {
    
    
    private PortBindings portBindings = null;
    
    private Configuration conf = null;
    
    private Map<String, String> parameters;
    
    @Mock
    private Connection mockedConnection;
    
    @Mock
    private PreparedStatement mockedPreparedStatement;

    
    private RdbDataCleaner process = new RdbDataCleaner() {
        
        protected Connection connect(String rdbUrl, String rdbUsername, String rdbPassword) throws java.sql.SQLException {
            return mockedConnection; 
        };
    };
    
    @Before
    public void init() {
        parameters = new HashMap<>();
        parameters.put(RdbDataCleaner.PARAM_TABLE_NAMES_CSV, "table1");
        parameters.put(RdbDataCleaner.PARAM_RDB_URL, "someUrl");
        parameters.put(RdbDataCleaner.PARAM_RDB_USERNAME, "username");
        parameters.put(RdbDataCleaner.PARAM_RDB_PASSWORD, "pass");
    }

    // ------------------------------------- TESTS ----------------------------------------------
    
    @Test
    public void testGetInputPorts() {
        // execute & assert
        assertNotNull(process.getInputPorts());
        assertEquals(0, process.getInputPorts().size());
    }
    
    @Test
    public void testGetOutputPorts() {
        // execute & assert
        assertNotNull(process.getOutputPorts());
        assertEquals(0, process.getOutputPorts().size());
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingTableNames() throws Exception {
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingRdbUrl() throws Exception {
        // given
        parameters.remove(RdbDataCleaner.PARAM_RDB_URL);
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingRdbUsername() throws Exception {
        // given
        parameters.remove(RdbDataCleaner.PARAM_RDB_USERNAME);
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingRdbPassword() throws Exception {
        // given
        parameters.remove(RdbDataCleaner.PARAM_RDB_PASSWORD);
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunInvalidTableNames() throws Exception {
        // given
        parameters.put(RdbDataCleaner.PARAM_TABLE_NAMES_CSV, "invalid table,validTable");
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRunOnSingleTable() throws Exception {
        // given
        when(mockedConnection.prepareStatement("DELETE FROM table1")).thenReturn(mockedPreparedStatement);
        when(mockedPreparedStatement.executeUpdate()).thenReturn(1);
        
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRunOnMultipleTablesWithSpaces() throws Exception {
        // given
        parameters.put(RdbDataCleaner.PARAM_TABLE_NAMES_CSV, "table1 ,table2 ");
        when(mockedConnection.prepareStatement("DELETE FROM table1")).thenReturn(mockedPreparedStatement);
        when(mockedConnection.prepareStatement("DELETE FROM table2")).thenReturn(mockedPreparedStatement);
        when(mockedPreparedStatement.executeUpdate()).thenReturn(1);
        
        // execute
        process.run(portBindings, conf, parameters);
    }
}
