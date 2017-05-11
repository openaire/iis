package eu.dnetlib.maven.plugin.properties;

import static eu.dnetlib.maven.plugin.properties.GenerateOoziePropertiesMojo.PROPERTY_NAME_SANDBOX_NAME;
import static eu.dnetlib.maven.plugin.properties.GenerateOoziePropertiesMojo.PROPERTY_NAME_WF_SOURCE_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class GenerateOoziePropertiesMojoTest {

    private GenerateOoziePropertiesMojo mojo = new GenerateOoziePropertiesMojo();
    
    @Before
    public void clearSystemProperties() {
        System.clearProperty(PROPERTY_NAME_SANDBOX_NAME);
        System.clearProperty(PROPERTY_NAME_WF_SOURCE_DIR);
    }
    
    @Test
    public void testExecuteEmpty() throws Exception {
        // execute
        mojo.execute();
        
        // assert
        assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }

    @Test
    public void testExecuteSandboxNameAlreadySet() throws Exception {
        // given
        String workflowSourceDir = "eu/dnetlib/iis/wf/transformers";
        String sandboxName = "originalSandboxName";
        System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
        System.setProperty(PROPERTY_NAME_SANDBOX_NAME, sandboxName);
        
        // execute
        mojo.execute();
        
        // assert
        assertEquals(sandboxName, System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }
    
    @Test
    public void testExecuteEmptyWorkflowSourceDir() throws Exception {
        // given
        String workflowSourceDir = "";
        System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
        
        // execute
        mojo.execute();
        
        // assert
        assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }
    
    @Test
    public void testExecuteNullSandboxNameGenerated() throws Exception {
        // given
        String workflowSourceDir = "eu/dnetlib/iis/";
        System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
        
        // execute
        mojo.execute();
        
        // assert
        assertNull(System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }
    
    @Test
    public void testExecute() throws Exception {
        // given
        String workflowSourceDir = "eu/dnetlib/iis/wf/transformers";
        System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
        
        // execute
        mojo.execute();
        
        // assert
        assertEquals("wf/transformers", System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }
    
    @Test
    public void testExecuteWithoutRoot() throws Exception {
        // given
        String workflowSourceDir = "wf/transformers";
        System.setProperty(PROPERTY_NAME_WF_SOURCE_DIR, workflowSourceDir);
        
        // execute
        mojo.execute();
        
        // assert
        assertEquals("wf/transformers", System.getProperty(PROPERTY_NAME_SANDBOX_NAME));
    }
    
}
