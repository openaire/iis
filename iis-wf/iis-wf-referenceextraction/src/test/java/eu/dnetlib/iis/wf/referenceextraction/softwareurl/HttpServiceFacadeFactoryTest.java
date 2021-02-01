package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import com.google.common.collect.Maps;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpServiceFacadeFactoryTest {

    @Test
    @DisplayName("Http service facade factory instantiates http service facade with default parameters")
    public void givenHttpServiceFacadeFactory_whenInstantiated_thenServiceWithDefaultParametersIsCreated() {
        HttpServiceFacadeFactory factory = new HttpServiceFacadeFactory();

        FacadeContentRetriever<String, String> service = factory.instantiate(Maps.newHashMap());

        assertNotNull(service);
        assertTrue(service instanceof HttpServiceFacade);
    }
}