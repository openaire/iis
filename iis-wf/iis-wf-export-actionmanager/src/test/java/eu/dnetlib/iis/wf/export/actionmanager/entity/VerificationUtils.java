package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * Utility methods useful for assertion validation.
 * @author mhorst
 *
 */
public final class VerificationUtils {

    
    // ------------------------------- CONSTRUCTORS -----------------------------
    
    private VerificationUtils() {}

    
    // ------------------------------- LOGIC ------------------------------------
    

    /**
     * Retrieves properties stored by entity exporter process.
     */
    public static Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty(AbstractEntityExporterProcess.OOZIE_ACTION_OUTPUT_FILENAME)));
        return properties;
    }
    
    /**
     * Verifies execution report.
     */
    public static void verifyReport(int expectedTotal, int expectedMissing) throws FileNotFoundException, IOException {
        Properties reportProperties = getStoredProperties();
        assertEquals(2, reportProperties.size());
        assertEquals(expectedTotal, Integer.parseInt(reportProperties.getProperty(AbstractEntityExporterProcess.TOTAL_ENTITIES_COUNTER_NAME)));
        assertEquals(expectedMissing, Integer.parseInt(reportProperties.getProperty(AbstractEntityExporterProcess.MISSING_ENTITIES_COUNTER_NAME)));
    }

    
    /**
     * Verifies entity action.
     */
    public static void verifyAction(AtomicAction action, String actionSetId, String targetColumn, String targetColumnFamily) {
        assertEquals(actionSetId, action.getRawSet());
        assertTrue(action.getTargetValue().length > 0);
        assertTrue(StringUtils.isNotBlank(action.getRowKey()));
        assertTrue(StringUtils.isNotBlank(action.getTargetRowKey()));
        assertEquals(targetColumn, action.getTargetColumn());
        assertEquals(targetColumnFamily, action.getTargetColumnFamily());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT, action.getAgent());
    }
    
}
