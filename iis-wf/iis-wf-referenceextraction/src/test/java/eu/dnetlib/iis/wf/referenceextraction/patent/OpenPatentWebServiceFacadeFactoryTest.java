package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_KEY;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_SECRET;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_READ_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * {@link OpenPatentWebServiceFacadeFactory} test class.
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacadeFactoryTest {

    @Test
    public void testCreate() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        
        // execute
        PatentServiceFacade service = factory.instantiate(conf);
        
        // assert
        assertNotNull(service);
        assertTrue(service instanceof OpenPatentWebServiceFacade);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidConnectionTimeout() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.put(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT, "non-int");
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidReadTimeout() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.put(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "non-int");
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingConsumerKey() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_CONSUMER_KEY);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthHost() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_AUTH_HOST);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthPort() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_AUTH_PORT);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidAuthPort() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.put(PARAM_SERVICE_ENDPOINT_AUTH_PORT, "non-int");
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthScheme() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthUriRoot() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsHost() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_OPS_HOST);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsPort() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_OPS_PORT);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidOpsPort() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.put(PARAM_SERVICE_ENDPOINT_OPS_PORT, "non-int");
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsScheme() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_OPS_SCHEME);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsUriRoot() {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Map<String, String> conf = prepareValidConfiguration();
        conf.remove(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT);
        
        // execute
        factory.instantiate(conf);
    }
    
    @Test
    public void testBuildCredential() {
        // given
        String key = "someKey";
        String secret = "someSecret";
        
        // execute
        String credential = OpenPatentWebServiceFacadeFactory.buildCredential(key, secret);
        
        // assert
        assertNotNull(credential);
        assertEquals(Base64.getEncoder().encodeToString((key+':'+secret).getBytes(StandardCharsets.UTF_8)), credential);
    }
    
    private Map<String, String> prepareValidConfiguration() {
        Map<String, String> conf = Maps.newHashMap();
        
        conf.put(PARAM_CONSUMER_KEY, "key");
        conf.put(PARAM_CONSUMER_SECRET, "secret");
        
        conf.put(PARAM_SERVICE_ENDPOINT_AUTH_HOST, "ops.epo.org");
        conf.put(PARAM_SERVICE_ENDPOINT_AUTH_PORT, "443");
        conf.put(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME, "https");
        conf.put(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT, "/3.2/auth/accesstoken");
        
        conf.put(PARAM_SERVICE_ENDPOINT_OPS_HOST, "ops.epo.org");
        conf.put(PARAM_SERVICE_ENDPOINT_OPS_PORT, "443");
        conf.put(PARAM_SERVICE_ENDPOINT_OPS_SCHEME, "https");
        conf.put(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT, "/3.2/rest-services/published-data/publication/docdb");
        
        return conf;
    }
    
}
