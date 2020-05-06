package eu.dnetlib.iis.wf.referenceextraction.patent;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_KEY;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_SECRET;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_READ_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.junit.Test;

/**
 * {@link OpenPatentWebServiceFacadeFactory} test class.
 * @author mhorst
 *
 */
public class OpenPatentWebServiceFacadeFactoryTest {

    @Test
    public void testCreate() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        
        // execute
        PatentServiceFacade service = factory.create(conf);
        
        // assert
        assertNotNull(service);
        assertTrue(service instanceof OpenPatentWebServiceFacade);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidConnectionTimeout() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.set(PARAM_SERVICE_ENDPOINT_CONNECTION_TIMEOUT, "non-int");
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidReadTimeout() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.set(PARAM_SERVICE_ENDPOINT_READ_TIMEOUT, "non-int");
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingConsumerKey() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_CONSUMER_KEY);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthHost() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_AUTH_HOST);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthPort() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_AUTH_PORT);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidAuthPort() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_PORT, "non-int");
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthScheme() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingAuthUriRoot() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsHost() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_OPS_HOST);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsPort() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_OPS_PORT);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testCreateInvalidOpsPort() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_PORT, "non-int");
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsScheme() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_OPS_SCHEME);
        
        // execute
        factory.create(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMissingOpsUriRoot() throws Exception {
        // given
        OpenPatentWebServiceFacadeFactory factory = new OpenPatentWebServiceFacadeFactory();
        Configuration conf = prepareValidConfiguration();
        conf.unset(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT);
        
        // execute
        factory.create(conf);
    }
    
    @Test
    public void testBuildCredential() throws Exception {
        // given
        String key = "someKey";
        String secret = "someSecret";
        
        // execute
        String credential = OpenPatentWebServiceFacadeFactory.buildCredential(key, secret);
        
        // assert
        assertNotNull(credential);
        assertEquals(Base64.getEncoder().encodeToString((key+':'+secret).getBytes(StandardCharsets.UTF_8)), credential);
    }
    
    @Test
    public void testBuildHttpClient() throws Exception {
     // given
        int connectionTimeout = 1;
        int readTimeout = 2;
        
        // execute
        HttpClient client = OpenPatentWebServiceFacadeFactory.buildHttpClient(connectionTimeout, readTimeout);
        
        // assert
        assertNotNull(client);
        assertTrue(client.getParams() instanceof BasicHttpParams);
        assertEquals(connectionTimeout, HttpConnectionParams.getConnectionTimeout(client.getParams()));
        assertEquals(readTimeout, HttpConnectionParams.getSoTimeout(client.getParams()));
    }
    
    
    private Configuration prepareValidConfiguration() {
        Configuration conf = new Configuration();
        
        conf.set(PARAM_CONSUMER_KEY, "key");
        conf.set(PARAM_CONSUMER_SECRET, "secret");
        
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_HOST, "ops.epo.org");
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_PORT, "443");
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_SCHEME, "https");
        conf.set(PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT, "/3.2/auth/accesstoken");
        
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_HOST, "ops.epo.org");
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_PORT, "443");
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_SCHEME, "https");
        conf.set(PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT, "/3.2/rest-services/published-data/publication/docdb");
        
        return conf;
    }
    
}
