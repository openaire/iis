package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class WebServiceMDStoreFacadeFactoryTest {
    
    private WebServiceMDStoreFacadeFactory factory = new WebServiceMDStoreFacadeFactory();

    private Map<String, String> parameters = new HashMap<>();
    
    // ---------------------------- TESTS ---------------------------------
    
    @Test(expected=InvalidParameterException.class)
    public void testCreateWithMissingServiceLocation() throws Exception {
        factory.create(parameters);
    }
    
    @Test(expected=InvalidParameterException.class)
    public void testCreateWithUndefinedServiceLocation() throws Exception {
        parameters.put(EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION, UNDEFINED_NONEMPTY_VALUE);
        factory.create(parameters);
    }
    
//    #2810#note-10 current cxf version does not seem to allow instantiating facade due to VerifyError     
//    @Test
//    public void testCreate() throws Exception {
//        parameters.put(EXPORT_ENTITY_MDSTORE_SERVICE_LOCATION, "localhost");
//        MDStoreFacade facade = factory.create(parameters);
//        assertNotNull(facade);
//    }

}
