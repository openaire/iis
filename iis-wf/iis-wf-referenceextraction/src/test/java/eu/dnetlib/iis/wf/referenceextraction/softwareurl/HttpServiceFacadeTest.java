package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.RetryLimitExceededException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class HttpServiceFacadeTest {

    private int connectionTimeout = 10000;

    private int readTimeout = 20000;

    private int maxPageContentLength = 1000000;

    private long throttleSleepTime = 1;

    private int maxRetriesCount = 2;

    @Mock
    private CloseableHttpClient httpClient;

    @Nested
    public class HttpServiceFacadeWithHTTP200FamilyTest {

        @Test
        @DisplayName("Http service facade retrieves content successfully for HTTP 200 server reply and valid entity")
        public void testGetContentForHttp200() throws Exception {
            // given
            String expectedResult = "this is expected result";
            HttpServiceFacade service = prepareValidService();

            // content retrieval mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            HttpEntity getContentHttpEntity = mock(HttpEntity.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(200);
            when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
            when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(expectedResult, response.getContent());
        }

        @Test
        @DisplayName("Http service facade retrieves truncated content successfully for HTTP 200 server reply and valid entity")
        public void testGetContentForHttp200WithTruncation() throws Exception {
            // given
            String content = "this is content line\nthis is content line";
            HttpServiceFacade service = new HttpServiceFacade(connectionTimeout, readTimeout, content.length() / 2 - 1, throttleSleepTime,
                    maxRetriesCount) {

                private static final long serialVersionUID = 1L;

                @Override
                protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
                    return httpClient;
                }
            };

            // content retrieval mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            HttpEntity getContentHttpEntity = mock(HttpEntity.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(200);
            when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
            when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(content.getBytes()));
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals("this is content line", response.getContent());
        }

        @Test
        @DisplayName("Http service facade does not retrieve content for HTTP 200 server reply and null entity")
        public void testGetContentForHttp200WithNullEntity() throws Exception {
            // given
            HttpServiceFacade service = prepareValidService();

            // content retrieval mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(200);
            when(getContentHttpResponse.getEntity()).thenReturn(null);
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class, response.getClass());
            assertEquals(HttpServiceFacadeException.class, response.getException().getClass());
        }
    }

    @Nested
    public class HttpServiceFacadeWithHTTP300FamilyTest {

        @Test
        @DisplayName("Http service facade returns persistent failure for HTTP 301 server reply and no location header")
        public void testGetMovedContentForHttp301NoLocationHeader() throws Exception {
            // given
            HttpServiceFacade service = prepareValidService();

            // initial response mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(301);
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class, response.getClass());
            assertEquals(HttpServiceFacadeException.class, response.getException().getClass());
        }

        @Test
        @DisplayName("Http service facade retrieves content successfully for HTTP 301 followed by HTTP 200 server replies")
        public void testGetMovedContentForHttp301And200() throws Exception {
            // given
            String originalResult = "this is original result";
            String movedResult = "this is moved result";
            HttpServiceFacade service = prepareValidService();
            Header mockedHeader = mock(Header.class);
            when(mockedHeader.getName()).thenReturn("Location");
            when(mockedHeader.getValue()).thenReturn("newUrl");
            Header[] headers = new Header[]{mockedHeader};

            // initial response mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(301);
            when(getContentHttpResponse.getAllHeaders()).thenReturn(headers);

            // moved response mock
            CloseableHttpResponse getMovedContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getMovedContentStatusLine = mock(StatusLine.class);
            HttpEntity getMovedContentHttpEntity = mock(HttpEntity.class);
            when(getMovedContentHttpResponse.getStatusLine()).thenReturn(getMovedContentStatusLine);
            when(getMovedContentStatusLine.getStatusCode()).thenReturn(200);
            when(getMovedContentHttpResponse.getEntity()).thenReturn(getMovedContentHttpEntity);
            when(getMovedContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(movedResult.getBytes()));

            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("newUrl")))).thenReturn(getMovedContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(movedResult, response.getContent());
        }
    }

    @Nested
    public class HttpServiceFacadeWithHTTP400FamilyTest {

        @Test
        @DisplayName("Http service facade returns persistent failure for HTTP 404 server reply")
        public void testGetNoContentForHttp404() throws Exception {
            // given
            HttpServiceFacade service = prepareValidService();

            // content retrieval mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(404);
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class, response.getClass());
            assertEquals(HttpServiceFacadeException.class, response.getException().getClass());
        }

        @Test
        @DisplayName("Http service facade returns transient failure for HTTP 429 server reply")
        public void testGetContentForHttp429RetryLimitExceeded() throws Exception {
            // given
            HttpServiceFacade service = prepareValidService();

            // rate-limited response mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(429);
            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
            assertEquals(RetryLimitExceededException.class, response.getException().getClass());
        }

        @Test
        @DisplayName("Http service facade retrieves content successfully for HTTP 429 followed by HTTP 200 server replies")
        public void testGetContentForHttp429And200() throws Exception {
            // given
            String validResult = "this is valid result";
            HttpServiceFacade service = prepareValidService();

            // rate-limited response mock
            CloseableHttpResponse getRateLimitedContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getRateLimitedContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(429);

            // moved response mock
            CloseableHttpResponse getValidContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getMovedContentStatusLine = mock(StatusLine.class);
            HttpEntity getMovedContentHttpEntity = mock(HttpEntity.class);
            when(getValidContentHttpResponse.getStatusLine()).thenReturn(getMovedContentStatusLine);
            when(getMovedContentStatusLine.getStatusCode()).thenReturn(200);
            when(getValidContentHttpResponse.getEntity()).thenReturn(getMovedContentHttpEntity);
            when(getMovedContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(validResult.getBytes()));

            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getRateLimitedContentHttpResponse,
                    getValidContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.Success.class, response.getClass());
            assertEquals(validResult, response.getContent());
        }
    }

    @Nested
    public class HttpServiceFacadeWithHTTP500FamilyTest {

        @Test
        @DisplayName("Http service facade returns transient failure for HTTP 500 server reply")
        public void testGetContentResultsInExceptionForHttp500() throws Exception {
            // given
            HttpServiceFacade service = prepareValidService();

            // content retrieval mock
            CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
            StatusLine getContentStatusLine = mock(StatusLine.class);
            when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
            when(getContentStatusLine.getStatusCode()).thenReturn(500);

            when(httpClient.execute(argThat(isHttpGETAndMatchesURL("someUrl")))).thenReturn(getContentHttpResponse);

            // execute
            FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

            // assert
            assertNotNull(response);
            assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
            assertEquals(HttpServiceFacadeException.class, response.getException().getClass());
        }
    }

    @Test
    @DisplayName("Http service facade returns transient failure when content retrieval throws an exception")
    public void testGetContentWithAnException() {
        // given
        HttpServiceFacade service = new HttpServiceFacade(connectionTimeout, readTimeout, maxPageContentLength, throttleSleepTime,
                maxRetriesCount) {
            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                throw new RuntimeException("failed");
            }
        };

        // execute
        FacadeContentRetrieverResponse<String> response = service.retrieveContent("someUrl");

        // assert
        assertNotNull(response);
        assertEquals(FacadeContentRetrieverResponse.TransientFailure.class, response.getClass());
        assertEquals(RuntimeException.class, response.getException().getClass());
    }

    @Test
    @DisplayName("Http service facade is serializable")
    public void testSerializeAndDeserialize() throws Exception {
        // given
        HttpServiceFacade service = new HttpServiceFacade(connectionTimeout, readTimeout, maxPageContentLength,
                throttleSleepTime, maxRetriesCount);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);

        // execute
        oos.writeObject(service);
        oos.flush();
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        HttpServiceFacade deserService = (HttpServiceFacade) ois.readObject();
        ois.close();

        // assert
        assertNotNull(deserService);
    }

    private HttpServiceFacade prepareValidService() {
        return new HttpServiceFacade(connectionTimeout, readTimeout, maxPageContentLength, throttleSleepTime,
                maxRetriesCount) {

            @Override
            protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
                return httpClient;
            }
        };
    }

    private static ArgumentMatcher<HttpUriRequest> isHttpGETAndMatchesURL(String url) {
        return argument -> Objects.nonNull(argument) &&
                HttpGet.METHOD_NAME.equals(argument.getMethod()) && url.equals(argument.getURI().toString());
    }
}
