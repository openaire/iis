package eu.dnetlib.iis.wf.referenceextraction.patent;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.RetryLimitExceededException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OpenPatentWebServiceFacadeTest {
    private final String consumerCredential = "myCredential";
    
    private final HttpHost authHost = new HttpHost("someAuthHost", 443, "https");
    private final HttpHost opsHost = new HttpHost("someOpsHost", 443, "https");
    private final String authUriRoot = "/auth_uri";
    private final String opsUriRoot = "ops_uri"; 
    
    @Mock
    private CloseableHttpClient httpClient;

    @Test
    @DisplayName("Open patent web service facade builds url to query")
    public void testGetPatentMetadataUrl() {
        // given
        ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
        patentBuilder.setPublnAuth("pubAuth");
        patentBuilder.setPublnKind("pubKind");
        patentBuilder.setPublnNr("pubNr");
        OpenPatentWebServiceFacade service = prepareValidService();

        // execute
        String url = service.buildUrl(patentBuilder.build());

        // assert
        assertNotNull(url);
        assertEquals(opsUriRoot + "/" + patentBuilder.getPublnAuth() + "." + patentBuilder.getPublnNr() + "."
                + patentBuilder.getPublnKind() + "/biblio", url);
    }
    
    @Test
    @DisplayName("Open patent web service facade build http request for patent")
    public void testBuildPatentMetaRequest() {
        // given
        String url = "/url/to/patent";
        String bearerToken = "someToken";
        
        // execute
        HttpRequest httpRequest = OpenPatentWebServiceFacade.buildPatentMetaRequest(url, bearerToken);

        // assert
        assertNotNull(httpRequest);
        assertEquals(url, httpRequest.getRequestLine().getUri());
        assertEquals("Bearer " + bearerToken, httpRequest.getLastHeader("Authorization").getValue());
    }
    
    @Test
    @DisplayName("Open patent web service facade builds authorization request")
    public void testBuildAuthRequest() throws Exception {
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
    @DisplayName("Open patent web service facade reauthentication works correctly")
    public void testReauthenticate() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        Gson gson = new Gson();
        String pageContents = gson.toJson(new AuthenticationResponse(accessToken));
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(httpEntity.getContent()).thenReturn(pageInputStream);
        
        // execute
        service.reauthenticate();
        String token = service.getSecurityToken();
        
        // assert
        assertNotNull(token);
        assertEquals(accessToken, token);
    }
    
    @Test
    @DisplayName("Open patent web service facade reauthentication throws exception")
    public void testReauthenticateResultingNon200() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        
        when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(404);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        // execute
        assertThrows(PatentWebServiceFacadeException.class, service::reauthenticate);
    }
    
    @Test
    @DisplayName("Open patent web service facade security token creation works correctly")
    public void testGetSecurityToken() throws Exception {
        // given
        OpenPatentWebServiceFacade service = prepareValidService();
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        String accessToken = "someAccessToken";
        
        when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(httpResponse);
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
        verify(httpClient, times(1)).execute(any(HttpHost.class),any(HttpRequest.class));
    }

    @Nested
    public class OpenPatentWebServiceFacadeWithHTTP200FamilyTest {

        @Test
        @DisplayName("Open patent web service facade retrieves content successfully for HTTP 200 server reply and valid entity")
        public void testGetPatentMetadataForHttp200() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            String expectedResult = "this is expected result";
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
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
            CloseableHttpResponse getPatentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine = mock(StatusLine.class);
            HttpEntity getPatentHttpEntity = mock(HttpEntity.class);
            when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
            when(getPatentStatusLine.getStatusCode()).thenReturn(200);
            when(getPatentHttpResponse.getEntity()).thenReturn(getPatentHttpEntity);
            when(getPatentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));

            when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(expectedResult, response.getContent());
        }

        @Test
        @DisplayName("Open patent web service facade returns persistent failure for HTTP 200 server reply and null entity")
        public void testGetPatentMetadataForHttp200AndNullEntity() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            String expectedResult = "this is expected result";
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
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
            CloseableHttpResponse getPatentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine = mock(StatusLine.class);
            when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
            when(getPatentStatusLine.getStatusCode()).thenReturn(200);
            when(getPatentHttpResponse.getEntity()).thenReturn(null);

            when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class, response.getClass());
            assertEquals(PatentWebServiceFacadeException.class, response.getException().getClass());
        }
    }

    @Nested
    public class OpenPatentWebServiceFacadeWithHTTP400FamilyTest {

        @Test
        @DisplayName("Open patent web service facade retrieves content successfully for HTTP 400 followed by HTTP 200 server replies and valid entity")
        public void testGetPatentMetadataForHttp400() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            String expectedResult = "this is expected result";
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            String accessToken = "someAccessToken";
            Gson gson = new Gson();
            String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

            CloseableHttpResponse authnHttpResponse1 = mock(CloseableHttpResponse.class);
            StatusLine authnStatusLine = mock(StatusLine.class);
            HttpEntity authnHttpEntity1 = mock(HttpEntity.class);

            when(authnHttpResponse1.getStatusLine()).thenReturn(authnStatusLine);
            when(authnStatusLine.getStatusCode()).thenReturn(200);
            when(authnHttpResponse1.getEntity()).thenReturn(authnHttpEntity1);
            when(authnHttpEntity1.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));

            CloseableHttpResponse authnHttpResponse2 = mock(CloseableHttpResponse.class);
            HttpEntity authnHttpEntity2 = mock(HttpEntity.class);

            when(authnHttpResponse2.getStatusLine()).thenReturn(authnStatusLine);
            when(authnHttpResponse2.getEntity()).thenReturn(authnHttpEntity2);
            when(authnHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));

            // metadata retrieval mock
            CloseableHttpResponse getPatentHttpResponse1 = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine1 = mock(StatusLine.class);
            when(getPatentHttpResponse1.getStatusLine()).thenReturn(getPatentStatusLine1);
            when(getPatentStatusLine1.getStatusCode()).thenReturn(400);

            CloseableHttpResponse getPatentHttpResponse2 = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine2 = mock(StatusLine.class);
            HttpEntity getPatentHttpEntity2 = mock(HttpEntity.class);
            when(getPatentHttpResponse2.getStatusLine()).thenReturn(getPatentStatusLine2);
            when(getPatentStatusLine2.getStatusCode()).thenReturn(200);
            when(getPatentHttpResponse2.getEntity()).thenReturn(getPatentHttpEntity2);
            when(getPatentHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));

            when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(authnHttpResponse1,
                    getPatentHttpResponse1, authnHttpResponse2, getPatentHttpResponse2);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(expectedResult, response.getContent());
        }

        @Test
        @DisplayName("Open patent web service facade retrieves content successfully for HTTP 403 followed by HTTP 200 server replies and valid entity")
        public void testGetPatentMetadataForHttp403() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            String expectedResult = "this is expected result";
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            String accessToken = "someAccessToken";
            Gson gson = new Gson();
            String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine authnStatusLine = mock(StatusLine.class);
            HttpEntity authnHttpEntity = mock(HttpEntity.class);

            when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
            when(authnStatusLine.getStatusCode()).thenReturn(200);
            when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
            when(authnHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));

            // metadata retrieval mock
            CloseableHttpResponse getPatentHttpResponse1 = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine1 = mock(StatusLine.class);
            when(getPatentHttpResponse1.getStatusLine()).thenReturn(getPatentStatusLine1);
            when(getPatentStatusLine1.getStatusCode()).thenReturn(403);

            CloseableHttpResponse getPatentHttpResponse2 = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine2 = mock(StatusLine.class);
            HttpEntity getPatentHttpEntity2 = mock(HttpEntity.class);
            when(getPatentHttpResponse2.getStatusLine()).thenReturn(getPatentStatusLine2);
            when(getPatentStatusLine2.getStatusCode()).thenReturn(200);
            when(getPatentHttpResponse2.getEntity()).thenReturn(getPatentHttpEntity2);
            when(getPatentHttpEntity2.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));

            when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(authnHttpResponse,
                    getPatentHttpResponse1, getPatentHttpResponse2);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(expectedResult, response.getContent());
        }

        @Test
        @DisplayName("Open patent web service facade returns transient failure when max retry limit is exceeded")
        public void testGetPatentMetadataForHttp403RetryCountExceeded() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            String accessToken = "someAccessToken";
            Gson gson = new Gson();
            String pageContents = gson.toJson(new AuthenticationResponse(accessToken));

            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine authnStatusLine = mock(StatusLine.class);
            HttpEntity authnHttpEntity = mock(HttpEntity.class);

            when(authnHttpResponse.getStatusLine()).thenReturn(authnStatusLine);
            when(authnStatusLine.getStatusCode()).thenReturn(200);
            when(authnHttpResponse.getEntity()).thenReturn(authnHttpEntity);
            when(authnHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(pageContents.getBytes()));

            // metadata retrieval mock
            CloseableHttpResponse getPatentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine = mock(StatusLine.class);
            when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
            when(getPatentStatusLine.getStatusCode()).thenReturn(403);

            when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(authnHttpResponse,
                    getPatentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
            assertEquals(RetryLimitExceededException.class, response.getException().getClass());
        }

        @Test
        @DisplayName("Open patent web service facade returns persistent failure for HTTP 404 server reply")
        public void testGetPatentMetadataForHttp404() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
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
            CloseableHttpResponse getPatentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine = mock(StatusLine.class);
            when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
            when(getPatentStatusLine.getStatusCode()).thenReturn(404);

            when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class, response.getClass());
            assertEquals(PatentWebServiceFacadeException.class, response.getException().getClass());
        }
    }

    @Nested
    public class OpenPatentWebServiceFacadeWithHTTP500FamilyTest {

        @Test
        @DisplayName("Open patent web service facade returns transient failure for HTTP 500 server reply")
        public void testGetPatentMetadataForHttp500() throws Exception {
            // given
            ImportedPatent.Builder patentBuilder = initializeWithDummyValues();
            OpenPatentWebServiceFacade service = prepareValidService();

            // authentication mock
            CloseableHttpResponse authnHttpResponse = mock(CloseableHttpResponse.class);
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
            CloseableHttpResponse getPatentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getPatentStatusLine = mock(StatusLine.class);
            when(getPatentHttpResponse.getStatusLine()).thenReturn(getPatentStatusLine);
            when(getPatentStatusLine.getStatusCode()).thenReturn(500);

            when(httpClient.execute(any(HttpHost.class),any(HttpRequest.class))).thenReturn(authnHttpResponse, getPatentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent(patentBuilder.build());

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
            assertEquals(PatentWebServiceFacadeException.class, response.getException().getClass());
        }
    }

    @Test
    @DisplayName("Open patent web service facade returns transient failure when content retrieval throws an exception")
    public void testGetPatentMetadataWithAnException() {
        // given
        OpenPatentWebServiceFacade service = new OpenPatentWebServiceFacade(httpClient, authHost, authUriRoot, opsHost,
                opsUriRoot, consumerCredential, 1, 1, new JsonParser()) {
            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                throw new RuntimeException("failed");
            }
        };

        // execute
        FacadeContentRetrieverResponse<String> response = service.retrieveContent(mock(ImportedPatent.class));

        // assert
        assertNotNull(response);
        assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
        assertEquals(RuntimeException.class, response.getException().getClass());
    }

    @Test
    @DisplayName("Open patent web service facade is serializable")
    public void testSerializeAndDeserialize() throws Exception {
        // given
        OpenPatentWebServiceFacade service = new OpenPatentWebServiceFacade(ConnectionDetailsBuilder.newBuilder()
                .withConnectionTimeout(10000).withReadTimeout(10000).withAuthHostName("authn-host").withAuthPort(8080)
                .withAuthScheme("https").withAuthUriRoot(authUriRoot).withOpsHostName("ops-host").withOpsPort(8090)
                .withOpsScheme("http").withOpsUriRoot(opsUriRoot).withConsumerCredential(consumerCredential)
                .withThrottleSleepTime(60000).withMaxRetriesCount(1).build());

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

    private static ImportedPatent.Builder initializeWithDummyValues() {
        ImportedPatent.Builder patentBuilder = ImportedPatent.newBuilder();
        String irrelevant = "irrelevant";
        patentBuilder.setApplnAuth(irrelevant);
        patentBuilder.setApplnNr(irrelevant);
        patentBuilder.setPublnAuth(irrelevant);
        patentBuilder.setPublnKind(irrelevant);
        patentBuilder.setPublnNr(irrelevant);
        return patentBuilder;
    }
    
    private static class AuthenticationResponse {
        
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
