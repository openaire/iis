package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.NoSuchElementException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.RetryLimitExceededException;

/**
 * {@link HttpContentRetriever} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpContentRetrieverTest {

    private int connectionTimeout = 10000;
    
    private int readTimeout = 20000;
    
    private int maxPageContentLength = 1000000;
    
    private long throttleSleepTime = 1;
    
    private int maxRetriesCount = 2;
    
    @Mock
    private CloseableHttpClient httpClient;
    
    @Test
    public void testGetContentForHttp200() throws Exception {
        // given
        String expectedResult = "this is expected result";
        HttpContentRetriever service = prepareValidService();
        
        // content retrieval mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        HttpEntity getContentHttpEntity = mock(HttpEntity.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(200);
        when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
        when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(expectedResult.getBytes()));
        
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertNull(response.getException());
        assertEquals(expectedResult, response.getContent());
    }
    
    @Test
    public void testGetNoContentForHttp404() throws Exception {
        // given
        HttpContentRetriever service = prepareValidService();
        
        // content retrieval mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(404);
        
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertEquals("", response.getContent());
        assertTrue(response.getException() instanceof NoSuchElementException);
    }
    
    @Test
    public void testGetContentResultsInExceptionForHttp500() throws Exception {
        // given
        HttpContentRetriever service = prepareValidService();
        
        // content retrieval mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(500);
        
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertEquals("", response.getContent());
        assertTrue(response.getException() instanceof RuntimeException);
    }
    
    @Test
    public void testGetMovedContentForHttp301NoLocationHeader() throws Exception {
        // given
        String originalResult = "this is original result";
        String movedResult = "this is moved result";
        HttpContentRetriever service = prepareValidService();

        // initial response mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        HttpEntity getContentHttpEntity = mock(HttpEntity.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(301);
        when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
        when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(originalResult.getBytes()));

        // moved response
        CloseableHttpResponse getMovedContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getMovedContentStatusLine = mock(StatusLine.class);
        HttpEntity getMovedContentHttpEntity = mock(HttpEntity.class);
        when(getMovedContentHttpResponse.getStatusLine()).thenReturn(getMovedContentStatusLine);
        when(getMovedContentStatusLine.getStatusCode()).thenReturn(200);
        when(getMovedContentHttpResponse.getEntity()).thenReturn(getMovedContentHttpEntity);
        when(getMovedContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(movedResult.getBytes()));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse, getMovedContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertTrue(response.getException() instanceof RuntimeException);
        assertEquals("", response.getContent());
    }
    
    @Test
    public void testGetMovedContentForHttp301And200() throws Exception {
        // given
        String originalResult = "this is original result";
        String movedResult = "this is moved result";
        HttpContentRetriever service = prepareValidService();
        Header mockedHeader = mock(Header.class);
        when(mockedHeader.getName()).thenReturn("Location");
        when(mockedHeader.getValue()).thenReturn("newUrl");
        Header[] headers = new Header[] {mockedHeader};
        
        // initial response mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        HttpEntity getContentHttpEntity = mock(HttpEntity.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(301);
        when(getContentHttpResponse.getAllHeaders()).thenReturn(headers);
        when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
        when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(originalResult.getBytes()));

        // moved response mock
        CloseableHttpResponse getMovedContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getMovedContentStatusLine = mock(StatusLine.class);
        HttpEntity getMovedContentHttpEntity = mock(HttpEntity.class);
        when(getMovedContentHttpResponse.getStatusLine()).thenReturn(getMovedContentStatusLine);
        when(getMovedContentStatusLine.getStatusCode()).thenReturn(200);
        when(getMovedContentHttpResponse.getEntity()).thenReturn(getMovedContentHttpEntity);
        when(getMovedContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(movedResult.getBytes()));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse, getMovedContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertNull(response.getException());
        assertEquals(movedResult, response.getContent());
    }
    
    @Test
    public void testGetContentForHttp429RetryLimitExceeded() throws Exception {
        // given
        HttpContentRetriever service = prepareValidService();
        
        // reate-limited response mock
        CloseableHttpResponse getContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        when(getContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(429);
        HttpEntity getContentHttpEntity = mock(HttpEntity.class);
        when(getContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
        when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream("".getBytes()));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertTrue(response.getException() instanceof RetryLimitExceededException);
        assertEquals("", response.getContent());
    }
    
    @Test
    public void testGetContentForHttp429And200() throws Exception {
        // given
        String validResult = "this is valid result";
        HttpContentRetriever service = prepareValidService();
        
        // rate-limited response mock
        CloseableHttpResponse getRateLimitedContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getContentStatusLine = mock(StatusLine.class);
        when(getRateLimitedContentHttpResponse.getStatusLine()).thenReturn(getContentStatusLine);
        when(getContentStatusLine.getStatusCode()).thenReturn(429);
        HttpEntity getContentHttpEntity = mock(HttpEntity.class);
        when(getRateLimitedContentHttpResponse.getEntity()).thenReturn(getContentHttpEntity);
        when(getContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream("".getBytes()));

        // moved response mock
        CloseableHttpResponse getValidContentHttpResponse = mock(CloseableHttpResponse.class);
        StatusLine getMovedContentStatusLine = mock(StatusLine.class);
        HttpEntity getMovedContentHttpEntity = mock(HttpEntity.class);
        when(getValidContentHttpResponse.getStatusLine()).thenReturn(getMovedContentStatusLine);
        when(getMovedContentStatusLine.getStatusCode()).thenReturn(200);
        when(getValidContentHttpResponse.getEntity()).thenReturn(getMovedContentHttpEntity);
        when(getMovedContentHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(validResult.getBytes()));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(getRateLimitedContentHttpResponse, getValidContentHttpResponse);
        
        // execute
        ContentRetrieverResponse response = service.retrieveUrlContent("someUrl");
        
        // assert
        assertNotNull(response);
        assertNull(response.getException());
        assertEquals(validResult, response.getContent());
    }
    
    @Test
    public void testSerializeAndDeserialize() throws Exception {
        // given
        HttpContentRetriever service = new HttpContentRetriever(connectionTimeout, readTimeout, maxPageContentLength,
                throttleSleepTime, maxRetriesCount);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        
        // execute
        oos.writeObject(service);
        oos.flush();
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        HttpContentRetriever deserService = (HttpContentRetriever) ois.readObject();
        ois.close();
        
        // assert
        assertNotNull(deserService);
    }
    
    private HttpContentRetriever prepareValidService() {
        return new HttpContentRetriever(connectionTimeout, readTimeout, maxPageContentLength, throttleSleepTime,
                maxRetriesCount) {

            private static final long serialVersionUID = 1L;

            protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
                return httpClient;
            }
        };
    }
    
}
