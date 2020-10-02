package eu.dnetlib.iis.common.java;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void testRunForEmptyArgs() {
        // given
        String[] args = new String[0];
        
        // execute
        assertThrows(CmdLineParserException.class, () -> ProcessWrapper.main(args));
    }
    
    @Test
    public void testRunForNonExistingProcessClass() {
        // given
        String[] args = new String[] {
                "non.existing.class"
        };
        
        // execute
        assertThrows(CmdLineParserException.class, () -> ProcessWrapper.main(args));
    }
    
    @Test
    public void testRunForNonProcessClass() {
        // given
        String[] args = new String[] {
                ProcessWrapper.class.getCanonicalName()
        };

        // execute
        assertThrows(CmdLineParserException.class, () -> ProcessWrapper.main(args));
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
