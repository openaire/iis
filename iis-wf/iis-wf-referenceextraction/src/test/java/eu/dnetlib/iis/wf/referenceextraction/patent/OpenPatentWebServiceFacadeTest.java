package eu.dnetlib.iis.wf.referenceextraction.patent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * {@link OpenPatentWebServiceFacade} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenPatentWebServiceFacadeTest {

    private final String consumerCredential = "myCredential";
    
    private final HttpHost authHost = new HttpHost("someAuthHost", 443, "https");
    private final HttpHost opsHost = new HttpHost("someOpsHost", 443, "https");
    private final String authUriRoot = "/auth_uri";
    private final String opsUriRoot = "ops_uri"; 
    
    @Mock
    private HttpClient httpClient;

    @Test
    public void testGetPatentMetadaUrl() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        patentBuilder.setPublnAuth("pubAuth");
        patentBuilder.setPublnKind("pubKind");
        patentBuilder.setPublnNr("pubNr");

        // execute
        String url = OpenPatentWebServiceFacade.getPatentMetaUrl(patentBuilder.build(), opsUriRoot);

        // assert
        assertNotNull(url);
        assertEquals(opsUriRoot + "/" + patentBuilder.getPublnAuth() + "." + patentBuilder.getPublnNr() + "."
                + patentBuilder.getPublnKind() + "/biblio", url);
    }
    
    @Test
    public void testBuildPatentMetaRequest() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        patentBuilder.setPublnAuth("pubAuth");
        patentBuilder.setPublnKind("pubKind");
        patentBuilder.setPublnNr("pubNr");

        String bearerToken = "someToken";
        
        // execute
        HttpRequest httpRequest = OpenPatentWebServiceFacade.buildPatentMetaRequest(patentBuilder.build(), bearerToken, opsUriRoot);

        // assert
        assertNotNull(httpRequest);
        assertEquals(OpenPatentWebServiceFacade.getPatentMetaUrl(patentBuilder.build(), opsUriRoot),
                httpRequest.getRequestLine().getUri());
        assertEquals("Bearer " + bearerToken, httpRequest.getLastHeader("Authorization").getValue());
    }
    
    @Test
    public void testbuildAuthRequest() throws Exception {
        // given
        String consumerCredential = "someCredential";
        
        // execute
        HttpRequest httpRequest = OpenPatentWebServiceFacade.buildAuthRequest(consumerCredential, authUriRoot);

        // assert
        assertNotNull(httpRequest);
        assertEquals(authUriRoot, httpRequest.getRequestLine().getUri());
        assertEquals("Basic " + consumerCredential, httpRequest.getLastHeader("Authorization").getValue());
        assertTrue(httpRequest instanceof HttpPost);
        HttpPost postRequest = (HttpPost) httpRequest;
        assertNotNull(postRequest.getEntity());
        assertTrue(postRequest.getEntity() instanceof UrlEncodedFormEntity);
        
        String content = IOUtils.toString(((UrlEncodedFormEntity)postRequest.getEntity()).getContent(), StandardCharsets.UTF_8);
        assertNotNull(content);
        assertEquals("grant_type=client_credentials", content);
        
    }
    
    @Test
    public void testReauthenticate() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(httpEntity.getContent()).thenReturn(pageInputStream);
        
        // execute
        String token = service.reauthenticate();
        
        // assert
        assertNotNull(token);
        assertEquals(accessToken, token);
    }
    
    @Test(expected = PatentServiceException.class)
    public void testReauthenticateResultingNon200() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(404);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        // execute
        service.reauthenticate();
    }
    
    
    @Test
    public void testGetSecurityToken() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(httpEntity.getContent()).thenReturn(pageInputStream);
        
        // execute
        String token = service.getSecurityToken();
        
        // assert
        assertNotNull(token);
        assertEquals(accessToken, token);
        
        // execute 2nd time
        token = service.getSecurityToken();
        
        // assert
        assertNotNull(token);
        assertEquals(accessToken, token);
        // 2nd call should not result in reauthentication, token should be returned straight away
        verify(httpClient, times(1)).execute(any(HttpHost.class),any(HttpGet.class));
    }
    
    @Test
    public void testGetPatentMetadataForHttp200() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        
        String expectedResult = "this is expected result";
        
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        HttpResponse authnHttpResponse = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(authnHttpEntity.getContent()).thenReturn(pageInputStream);
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse = mock(HttpResponse.class);
        StatusLine getPatentStatusLine = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity = mock(HttpEntity.class);
        when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
        when(getPatentStatusLine.getStatusCode()).thenReturn(200);
        when(getPatentHttpResponse.getEntity()).thenReturn(getPatentHttpEntity);
        when(getPatentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);
        
        // execute
        String patentContents = service.getPatentMetadata(patentBuilder.build());
        
        // assert
        assertNotNull(patentContents);
        assertEquals(expectedResult, patentContents);
    }
    
    
    @Test
    public void testGetPatentMetadataForHttp400() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        
        String expectedResult = "this is expected result";
        
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        String accessToken = "someAccessToken";
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

        HttpResponse authnHttpResponse1 = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity1 = mock(HttpEntity.class);
        
        when(authnHttpResponse1.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse1.getEntity()).thenReturn(authnHttpEntity1);
        when(authnHttpEntity1.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));
        
        HttpResponse authnHttpResponse2 = mock(HttpResponse.class);
        HttpEntity authnHttpEntity2 = mock(HttpEntity.class);
        
        when(authnHttpResponse2.getStatusLine()).thenReturn(authnStatusLine);
        when(authnHttpResponse2.getEntity()).thenReturn(authnHttpEntity2);
        when(authnHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse1 = mock(HttpResponse.class);
        StatusLine getPatentStatusLine1 = mock(StatusLine.class);
        when(getPatentHttpResponse1.getStatusLine()).thenReturn(getPatentStatusLine1);
        when(getPatentStatusLine1.getStatusCode()).thenReturn(400);
        
        HttpResponse getPatentHttpResponse2 = mock(HttpResponse.class);
        StatusLine getPatentStatusLine2 = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity2 = mock(HttpEntity.class);
        when(getPatentHttpResponse2.getStatusLine()).thenReturn(getPatentStatusLine2);
        when(getPatentStatusLine2.getStatusCode()).thenReturn(200);
        when(getPatentHttpResponse2.getEntity()).thenReturn(getPatentHttpEntity2);
        when(getPatentHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));
        
        when(httpClient.execute(any(HttpHost.class), any(HttpGet.class))).thenReturn(authnHttpResponse1,
                getPatentHttpResponse1, authnHttpResponse2, getPatentHttpResponse2);
        
        // execute
        String patentContents = service.getPatentMetadata(patentBuilder.build());
        
        // assert
        assertNotNull(patentContents);
        assertEquals(expectedResult, patentContents);
    }
    
    @Test(expected = NoSuchElementException.class)
    public void testGetPatentMetadataForHttp404() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        HttpResponse authnHttpResponse = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(authnHttpEntity.getContent()).thenReturn(pageInputStream);
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse = mock(HttpResponse.class);
        StatusLine getPatentStatusLine = mock(StatusLine.class);
        when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
        when(getPatentStatusLine.getStatusCode()).thenReturn(404);
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);
        
        // execute
        service.getPatentMetadata(patentBuilder.build());
    }
    
    @Test
    public void testGetPatentMetadataForHttp403() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        
        String expectedResult = "this is expected result";
        
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        String accessToken = "someAccessToken";
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

        HttpResponse authnHttpResponse = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity = mock(HttpEntity.class);
        
        when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
        when(authnHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse1 = mock(HttpResponse.class);
        StatusLine getPatentStatusLine1 = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity1 = mock(HttpEntity.class);
        when(getPatentHttpResponse1.getStatusLine()).thenReturn(getPatentStatusLine1);
        when(getPatentStatusLine1.getStatusCode()).thenReturn(403);
        when(getPatentHttpResponse1.getEntity()).thenReturn(getPatentHttpEntity1);
        
        HttpResponse getPatentHttpResponse2 = mock(HttpResponse.class);
        StatusLine getPatentStatusLine2 = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity2 = mock(HttpEntity.class);
        when(getPatentHttpResponse2.getStatusLine()).thenReturn(getPatentStatusLine2);
        when(getPatentStatusLine2.getStatusCode()).thenReturn(200);
        when(getPatentHttpResponse2.getEntity()).thenReturn(getPatentHttpEntity2);
        when(getPatentHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));
        
        when(httpClient.execute(any(HttpHost.class), any(HttpGet.class))).thenReturn(authnHttpResponse,
                getPatentHttpResponse1, getPatentHttpResponse2);
        
        // execute
        String patentContents = service.getPatentMetadata(patentBuilder.build());
        
        // assert
        assertNotNull(patentContents);
        assertEquals(expectedResult, patentContents);
    }
    
    @Test(expected = PatentServiceException.class)
    public void testGetPatentMetadataForHttp403RetryCountExceeded() throws Exception {
     // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        String accessToken = "someAccessToken";
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

        HttpResponse authnHttpResponse = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity = mock(HttpEntity.class);
        
        when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
        when(authnHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse = mock(HttpResponse.class);
        StatusLine getPatentStatusLine = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity = mock(HttpEntity.class);
        when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
        when(getPatentStatusLine.getStatusCode()).thenReturn(403);
        when(getPatentHttpResponse.getEntity()).thenReturn(getPatentHttpEntity);
        
        when(httpClient.execute(any(HttpHost.class), any(HttpGet.class))).thenReturn(authnHttpResponse,
                getPatentHttpResponse);
        
        // execute
        service.getPatentMetadata(patentBuilder.build());
    }
    
    @Test(expected = PatentServiceException.class)
    public void testGetPatentMetadataForHttp500() throws Exception {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        OpenPatentWebServiceFacade service = prepareValidService();
        
        // authentication mock
        HttpResponse authnHttpResponse = mock(HttpResponse.class);
        StatusLine authnStatusLine = mock(StatusLine.class);
        HttpEntity authnHttpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
        when(authnStatusLine.getStatusCode()).thenReturn(200);
        when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(authnHttpEntity.getContent()).thenReturn(pageInputStream);
        
        // metadata retrieval mock
        HttpResponse getPatentHttpResponse = mock(HttpResponse.class);
        StatusLine getPatentStatusLine = mock(StatusLine.class);
        HttpEntity getPatentHttpEntity = mock(HttpEntity.class);
        when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
        when(getPatentStatusLine.getStatusCode()).thenReturn(500);
        when(getPatentHttpResponse.getEntity()).thenReturn(getPatentHttpEntity);
        
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);
        
        // execute
        service.getPatentMetadata(patentBuilder.build());
    }
    
    @Test
    public void testBuildHttpClient() throws Exception {
     // given
        int connectionTimeout = 1;
        int readTimeout = 2;
        
        // execute
        HttpClient client = OpenPatentWebServiceFacade.buildHttpClient(connectionTimeout, readTimeout);
        
        // assert
        assertNotNull(client);
    }
    
    @Test
    public void testSerializeAndDeserialize() throws Exception {
        // given
        OpenPatentWebServiceFacade service = new OpenPatentWebServiceFacade(10000, 10000,
                "authn-host", 8080, "https", authUriRoot, 
                "ops-host", 8090, "http", opsUriRoot, 
                consumerCredential, 60000, 1);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        
        // execute
        oos.writeObject(service);
        oos.flush();
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        OpenPatentWebServiceFacade deserService = (OpenPatentWebServiceFacade) ois.readObject();
        ois.close();
        
        // assert
        assertNotNull(deserService);
    }
    
    private ImportedPatent.Builder initializeWithDummyValues() {
        ImportedPatent.Builder patentBuilder = ImportedPatent.newBuilder();
        String irrelevant = "irrelevant";
        patentBuilder.setApplnAuth(irrelevant);
        patentBuilder.setApplnNr(irrelevant);
        patentBuilder.setPublnAuth(irrelevant);
        patentBuilder.setPublnKind(irrelevant);
        patentBuilder.setPublnNr(irrelevant);
        return patentBuilder;
    }
    
    private class AuthenticationResponse {
        
        @SuppressWarnings("unused")
        private String access_token;
        
        public AuthenticationResponse(String access_token) {
            this.access_token = access_token;
        }

    }
    
    private OpenPatentWebServiceFacade prepareValidService() {
        return new OpenPatentWebServiceFacade(httpClient, authHost, authUriRoot, opsHost,
                opsUriRoot, consumerCredential, 1, 1, new JsonParser());
    }
    
}
