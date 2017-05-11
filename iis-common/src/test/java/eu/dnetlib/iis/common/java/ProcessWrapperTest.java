package eu.dnetlib.iis.common.java;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class ProcessWrapperTest {
    
    // ------------------------------------- TESTS ---------------------------------------
    
    @Test
    public void testGetConfiguration() throws Exception {
        // given
        ProcessWrapper processWrapper = new ProcessWrapper();
        
        // execute
        Configuration cfg = processWrapper.getConfiguration();
        
        // assert
        assertNotNull(cfg);
    }

    @Test(expected=CmdLineParserException.class)
    public void testRunForEmptyArgs() throws Exception {
        // given
        String[] args = new String[0];
        
        // execute
        ProcessWrapper.main(args);
    }
    
    @Test(expected=CmdLineParserException.class)
    public void testRunForNonExistingProcessClass() throws Exception {
        // given
        String[] args = new String[] {
                "non.existing.class"
        };
        
        // execute
        ProcessWrapper.main(args);
    }
    
    @Test(expected=CmdLineParserException.class)
    public void testRunForNonProcessClass() throws Exception {
        // given
        String[] args = new String[] {
                ProcessWrapper.class.getCanonicalName()
        };

        // execute
        ProcessWrapper.main(args);
    }
    
    @Test
    public void testRun() throws Exception {
        // given
        String[] args = new String[] {
                TestProcess.class.getCanonicalName()
        };

        // execute
        ProcessWrapper.main(args);
        
        // assert
        assertTrue(TestProcess.isExecuted());
    }
    
}
