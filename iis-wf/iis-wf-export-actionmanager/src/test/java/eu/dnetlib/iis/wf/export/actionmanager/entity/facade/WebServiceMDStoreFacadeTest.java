package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.data.mdstore.MDStoreService;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class WebServiceMDStoreFacadeTest {

    @Mock
    private MDStoreService service;
    
    @Captor
    private ArgumentCaptor<String> mdStoreIdCaptor;

    @Captor
    private ArgumentCaptor<String> recordIdCaptor;
    
    // ---------------------------- TESTS ---------------------------------
    
    @Test
    public void testInvocation() throws Exception {
        // given
        String mdStoreId = "md-id";
        String recordId = "record-id";
        WebServiceMDStoreFacade mdStoreFacade = new WebServiceMDStoreFacade(service);
        
        // execute
        mdStoreFacade.fetchRecord(mdStoreId, recordId);
        
        // assert
        verify(service).deliverRecord(mdStoreIdCaptor.capture(), recordIdCaptor.capture());
        assertEquals(mdStoreId, mdStoreIdCaptor.getValue());
        assertEquals(recordId, recordIdCaptor.getValue());
    }
    
    @Test
    public void testInitialization() throws Exception {
        // given
        String serviceLocation = "localhost";
        long readTimeout = 60000;
        long connectionTimeout = 60000;
        
        // execute
        WebServiceMDStoreFacade mdStoreFacade = new WebServiceMDStoreFacade(
                serviceLocation, readTimeout, connectionTimeout);
        
        // assert
        assertNotNull(mdStoreFacade);
    }

}
