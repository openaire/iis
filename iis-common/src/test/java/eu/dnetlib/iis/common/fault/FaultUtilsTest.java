package eu.dnetlib.iis.common.fault;

import eu.dnetlib.iis.audit.schemas.Cause;
import eu.dnetlib.iis.audit.schemas.Fault;
import org.junit.jupiter.api.Test;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author mhorst
 *
 */
public class FaultUtilsTest {

    @Test
    public void testExceptionToFault() throws Exception {
        // given
        String entityId = "id";
        Throwable causeThrowable = new InvalidParameterException("cause message");
        Throwable rootThrowable = new RuntimeException("root meesage", causeThrowable);

        String supplementaryDataKey = "auxKey";
        String supplementaryDataValue = "auxValue";
        Map<CharSequence, CharSequence> auditSupplementaryData = new HashMap<>();
        auditSupplementaryData.put(supplementaryDataKey, supplementaryDataValue);
        
        // execute
        Fault fault = FaultUtils.exceptionToFault(entityId, rootThrowable, auditSupplementaryData);
        
        // assert
        assertNotNull(fault);
        assertEquals(entityId, fault.getInputObjectId());
        assertNotNull(fault.getTimestamp());
        assertEquals(rootThrowable.getClass().getName(), fault.getCode());
        assertEquals(rootThrowable.getMessage(), fault.getMessage());
        assertNotNull(fault.getStackTrace());
        assertNotNull(fault.getSupplementaryData());
        assertEquals(1, fault.getSupplementaryData().size());
        assertEquals(supplementaryDataValue, fault.getSupplementaryData().get(supplementaryDataKey));
        
        assertNotNull(fault.getCauses());
        assertEquals(1, fault.getCauses().size());
        assertEquals(causeThrowable.getClass().getName(), fault.getCauses().get(0).getCode());
        assertEquals(causeThrowable.getMessage(), fault.getCauses().get(0).getMessage());
    }
    
    @Test
    public void testAppendThrowableToCauses() throws Exception {
        // given
        
        Throwable causeThrowable = new InvalidParameterException("cause message");
        Throwable rootThrowable = new RuntimeException("root meesage", causeThrowable);
        
        // execute
        List<Cause> causes = FaultUtils.appendThrowableToCauses(rootThrowable, new ArrayList<>());
        
        // assert
        assertEquals(2, causes.size());
        assertEquals(rootThrowable.getClass().getName(), causes.get(0).getCode());
        assertEquals(rootThrowable.getMessage(), causes.get(0).getMessage());
        assertEquals(causeThrowable.getClass().getName(), causes.get(1).getCode());
        assertEquals(causeThrowable.getMessage(), causes.get(1).getMessage());
    }

}
