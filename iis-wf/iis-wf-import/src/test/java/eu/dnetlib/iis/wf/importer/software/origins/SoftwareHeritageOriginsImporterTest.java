package eu.dnetlib.iis.wf.importer.software.origins;

import com.google.gson.Gson;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.*;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.*;
import static eu.dnetlib.iis.wf.importer.VerificationUtils.verifyReport;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * {@link SoftwareHeritageOriginsImporter} test class.
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class SoftwareHeritageOriginsImporterTest {
    
    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;

    @TempDir
    public File tempFolder;
    
    @Mock
    private CloseableHttpClient httpClient;
    
    @Mock
    private DataFileWriter<SoftwareHeritageOrigin> originWriter;
    
    @Captor
    private ArgumentCaptor<SoftwareHeritageOrigin> originCaptor;

    @Test
    public void testGetNextLinkFromHeaderSingle() throws Exception {
        // given
        String linkHeader = "</api/next>; rel=\"next\"";
        
        // execute
        String result = SoftwareHeritageOriginsImporter.getNextLinkFromHeader(linkHeader);
        
        // assert
        assertNotNull(result);
        assertEquals("/api/next", result);
    }
    
    @Test
    public void testGetNextLinkFromHeaderNullOrBlank() throws Exception {
        // execute & assert
        assertNull(SoftwareHeritageOriginsImporter.getNextLinkFromHeader(null));
        assertNull(SoftwareHeritageOriginsImporter.getNextLinkFromHeader(""));
    }
    
    @Test
    public void testGetNextLinkFromHeaderMultiple() throws Exception {
        // given
        String linkHeader = "</api/next>; rel=\"next\", </api/prev>; rel=\"prev\"";
        
        // execute
        String result = SoftwareHeritageOriginsImporter.getNextLinkFromHeader(linkHeader);
        
        // assert
        assertNotNull(result);
        assertEquals("/api/next", result);
    }
    
    @Test
    public void testGetNextLinkFromHeaders() throws Exception {
        // given
        Header header = new BasicHeader("Link", "</api/next>; rel=\"next\"");
        
        // execute
        String result = SoftwareHeritageOriginsImporter.getNextLinkFromHeaders(new Header[] {header});
        
        // assert
        assertNotNull(result);
        assertEquals("/api/next", result);
    }
    
    @Test
    public void testGetNextLinkFromHeadersNullEmptyOrUnsupported() throws Exception {
     // execute & assert
        assertNull(SoftwareHeritageOriginsImporter.getNextLinkFromHeaders(null));
        assertNull(SoftwareHeritageOriginsImporter.getNextLinkFromHeaders(new Header[0]));
        assertNull(SoftwareHeritageOriginsImporter.getNextLinkFromHeaders(new Header[] {new BasicHeader("unsupported", "value")}));
    }
    
    @Test
    public void testPrepareNextRequestUrlNotNull() throws Exception {
        // given
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        Header header = new BasicHeader("Link", "</api/next>; rel=\"next\"");
        when(httpResponse.getAllHeaders()).thenReturn(new Header[] {header});
        
        // execute
        HttpRequest result = SoftwareHeritageOriginsImporter.prepareNextRequest(httpResponse);
        
        // assert
        assertNotNull(result);
        assertEquals("/api/next", result.getRequestLine().getUri());
        
    }
    
    @Test
    public void testPrepareNextRequestUrlIsNull() throws Exception {
        // given
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        when(httpResponse.getAllHeaders()).thenReturn(null);
        
        // execute
        HttpRequest result = SoftwareHeritageOriginsImporter.prepareNextRequest(httpResponse);
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testConvertEntry() throws Exception {
        // given
        SoftwareHeritageOriginEntry source = buildSoftwareHeritageOriginEntry("someUrl");
        
        // execute
        SoftwareHeritageOrigin result = SoftwareHeritageOriginsImporter.convertEntry(source);
        
        // assert
        assertNotNull(result);
        assertEquals(source.getUrl(), result.getUrl());
    }
    
    @Test
    public void testParsePage() throws Exception {
        // given
        Gson gson = new Gson();
        SoftwareHeritageOriginEntry entry1 = buildSoftwareHeritageOriginEntry("someUrl1");
        SoftwareHeritageOriginEntry entry2 = buildSoftwareHeritageOriginEntry("someUrl2");

        // execute
        SoftwareHeritageOriginEntry[] results = SoftwareHeritageOriginsImporter.parsePage(
                gson.toJson(new SoftwareHeritageOriginEntry[] {entry1, entry2}), gson);
        
        // assert
        assertNotNull(results);
        assertEquals(2, results.length);
        assertEquals(entry1.getUrl(), results[0].getUrl());
        assertEquals(entry2.getUrl(), results[1].getUrl());
    }
    
    @Test
    public void testParseBlankPage() throws Exception {
        // given
        Gson gson = new Gson();

        // execute
        SoftwareHeritageOriginEntry[] results = SoftwareHeritageOriginsImporter.parsePage("", gson);
        
        // assert
        assertNotNull(results);
        assertEquals(0, results.length);
    }
    
    @Test
    public void testParseInvalidPage() throws Exception {
        // given
        Gson gson = new Gson();

        // execute
        assertThrows(RuntimeException.class, () -> SoftwareHeritageOriginsImporter.parsePage("invalid", gson));
    }
    
    @Test
    public void testBuildUri() throws Exception {
        // given
        String rootUri = "rootUriPart";
        int startElement = 1;
        int pageSize = 10;
        
        // execute
        String result = SoftwareHeritageOriginsImporter.buildUri(rootUri, startElement, pageSize);
        
        // assert
        assertNotNull(result);
        assertEquals("rootUriPart?page_token=1&origin_count=10", result);
    }
    
    @Test
    public void testStoreNextElementIndex() throws Exception {
        // given
        File propertyFile = Files.createTempFile(tempFolder.toPath(), "testStoreNextElementIndex", "tmp").toFile();
        System.setProperty(WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME, propertyFile.getAbsolutePath());
        int nextElementIndex = 2;
        
        // execute
        SoftwareHeritageOriginsImporter.storeNextElementIndex(nextElementIndex);
        
        // assert
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertyFile)) {
            props.load(fis);
            assertEquals(String.valueOf(nextElementIndex), props.getProperty(SoftwareHeritageOriginsImporter.OUTPUT_PROPERTY_NEXT_RECORD_INDEX));
        }
    }

    @Test
    public void testGetOutputPorts() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = new SoftwareHeritageOriginsImporter();
        
        // execute
        Map<String, PortType> result = importer.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(SoftwareHeritageOriginsImporter.PORT_OUT_ORIGINS));
        assertTrue(result.get(SoftwareHeritageOriginsImporter.PORT_OUT_ORIGINS) instanceof AvroPortType);
        assertSame(SoftwareHeritageOrigin.SCHEMA$, ((AvroPortType) result.get(SoftwareHeritageOriginsImporter.PORT_OUT_ORIGINS)).getSchema());
    }
    
    @Test
    public void testRunWithoutNextHeader() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse);
        StatusLine statusLine = mock(StatusLine.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        // preparing page contents
        SoftwareHeritageOriginEntry entry = buildSoftwareHeritageOriginEntry("someUrl1");
        Gson gson = new Gson();
        String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry});
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
        when(httpEntity.getContent()).thenReturn(pageInputStream);
        
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(1)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(1, origins.size());
        SoftwareHeritageOrigin origin = origins.get(0);
        assertNotNull(origin);
        assertEquals(entry.getUrl(), origin.getUrl());
        verifyReport(1, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
    }
    
    @Test
    public void testRunWithNextHeader() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        CloseableHttpResponse httpResponse2 = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse, httpResponse2);

        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        
        Gson gson = new Gson();
        SoftwareHeritageOriginEntry entry1 = buildSoftwareHeritageOriginEntry("someUrl1");
        SoftwareHeritageOriginEntry entry2 = buildSoftwareHeritageOriginEntry("someUrl2");
        
        //1st response
        {
            Header header = new BasicHeader("Link", "</api/next>; rel=\"next\"");
            when(httpResponse.getAllHeaders()).thenReturn(new Header[] {header});
            
            when(httpResponse.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry1});
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
    
        }
        //2nd response
        {
            when(httpResponse2.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse2.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry2});
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
        }
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(2)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(2, origins.size());
        assertNotNull(origins.get(0));
        assertEquals(entry1.getUrl(), origins.get(0).getUrl());
        assertNotNull(origins.get(1));
        assertEquals(entry2.getUrl(), origins.get(1).getUrl());
        verifyReport(2, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
    }
    
    @Test
    public void testRunWithRetryBecauseOfThrottling() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY, "1");
        
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        CloseableHttpResponse httpResponse2 = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse, httpResponse2);

        Gson gson = new Gson();
        SoftwareHeritageOriginEntry entry = buildSoftwareHeritageOriginEntry("someUrl1");
        
        //1st response
        {
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(429);
            when(httpResponse.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = "throttling";
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
    
        }
        //2nd response
        {
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(200);
            when(httpResponse2.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse2.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry});
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
        }
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(1)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(1, origins.size());
        assertNotNull(origins.get(0));
        assertEquals(entry.getUrl(), origins.get(0).getUrl());
        verifyReport(1, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
        
    }
    
    @Test
    public void testRunWithRetryBecauseOfServerError() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY, "1");
        
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        CloseableHttpResponse httpResponse2 = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse, httpResponse2);

        Gson gson = new Gson();
        SoftwareHeritageOriginEntry entry = buildSoftwareHeritageOriginEntry("someUrl1");
        
        //1st response
        {
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(503);
            when(httpResponse.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = "SERVER ERROR";
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
    
        }
        //2nd response
        {
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(200);
            when(httpResponse2.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse2.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry});
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
        }
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(1)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(1, origins.size());
        assertNotNull(origins.get(0));
        assertEquals(entry.getUrl(), origins.get(0).getUrl());
        verifyReport(1, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
    }
    
    @Test
    public void testRunWithRetryBecauseOfTimeoutException() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY, "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT, "1");
        
        CloseableHttpResponse httpResponse2 = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class), any(HttpGet.class))).thenThrow(SocketTimeoutException.class)
                .thenReturn(httpResponse2);

        Gson gson = new Gson();
        SoftwareHeritageOriginEntry entry = buildSoftwareHeritageOriginEntry("someUrl1");
        
        //response
        {
            StatusLine statusLine = mock(StatusLine.class);
            when(statusLine.getStatusCode()).thenReturn(200);
            when(httpResponse2.getStatusLine()).thenReturn(statusLine);
            
            HttpEntity httpEntity = mock(HttpEntity.class);
            when(httpResponse2.getEntity()).thenReturn(httpEntity);
            
            // preparing page contents
            String pageContents = gson.toJson(new SoftwareHeritageOriginEntry[] {entry});
            InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
            when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
            when(httpEntity.getContent()).thenReturn(pageInputStream);
        }
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(1)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(1, origins.size());
        assertNotNull(origins.get(0));
        assertEquals(entry.getUrl(), origins.get(0).getUrl());
        verifyReport(1, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
    }
    
    @Test
    public void testExceedTheAllowedRetriesAfterTimeoutException() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY, "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT, "1");
        
        when(httpClient.execute(any(HttpHost.class), any(HttpGet.class))).thenThrow(SocketTimeoutException.class);
        
        // execute
        assertThrows(SocketTimeoutException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunWithoutRetryBecauseOfServerError() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "1");
        this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT, "0");
        
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any(HttpHost.class),any(HttpGet.class))).thenReturn(httpResponse);
        
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(503);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        
        // preparing page contents
        String pageContents = "SERVER ERROR";
        InputStream pageInputStream = new ByteArrayInputStream(pageContents.getBytes());
        when(httpEntity.getContentLength()).thenReturn(Long.valueOf(pageContents.length()));
        when(httpEntity.getContent()).thenReturn(pageInputStream);

        // execute
        assertThrows(RuntimeException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointUriMissing() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams(null, "somehost.com", "https", "8080", "1");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointUriUndefinedAkaOfflineMode() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams(WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE, "somehost.com", "https", "8080", "1");
        
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(originWriter, times(0)).append(originCaptor.capture());
        List<SoftwareHeritageOrigin> origins = originCaptor.getAllValues();
        assertEquals(0, origins.size());
        verifyReport(0, SoftwareHeritageOriginsImporter.COUNTER_NAME_TOTAL);
    }
    
    @Test
    public void testRunEndpointHostMissing() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", null, "https", "8080", "1");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointSchemeMissing() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", null, "8080", "1");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointPortMissing() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", null, "1");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointPortInvalidValue() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "invalid", "1");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointStartIndexMissing() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", null);
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEndpointStartIndexInvalidValue() throws Exception {
        // given
        SoftwareHeritageOriginsImporter importer = initializeImporterParams("api/origins", "somehost.com", "https", "8080", "invalid");
        
        // execute
        assertThrows(NumberFormatException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    // ------------------------------ PRIVATE -------------------------------------
    
    private SoftwareHeritageOriginsImporter initializeImporterParams(String uriRoot, String host, String scheme, String port, 
            String startElementIndex) throws Exception {
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                tempFolder.getAbsolutePath() + File.separatorChar + "test.properties");
        
        Map<String, Path> output = new HashMap<>();
        output.put(SoftwareHeritageOriginsImporter.PORT_OUT_ORIGINS, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.portBindings = new PortBindings(Collections.emptyMap(), output);
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        if (uriRoot != null) {
            this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT, uriRoot);
        }
        if (host != null) {
            this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST, host);
        }
        if (scheme != null) {
            this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME, scheme);
        }
        if (port != null) {
            this.parameters.put(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT, port);
        }
        if (startElementIndex != null) {
            this.parameters.put(IMPORT_SOFTWARE_HERITAGE_START_INDEX, startElementIndex);
        }
        
        return new SoftwareHeritageOriginsImporter() {
            
            @Override
            protected DataFileWriter<SoftwareHeritageOrigin> getWriter(FileSystem fs, PortBindings portBindings) throws IOException {
                return originWriter;
            }
            
            @Override
            protected CloseableHttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
                return httpClient;
            }
            
        };
    }
    
    private SoftwareHeritageOriginEntry buildSoftwareHeritageOriginEntry(String url) {
        SoftwareHeritageOriginEntry entry = new SoftwareHeritageOriginEntry();
        entry.setUrl(url);
        return entry;
    }
    
}
