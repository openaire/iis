package eu.dnetlib.iis.wf.importer.software.origins;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_READ_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_PAGE_SIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SOFTWARE_HERITAGE_START_INDEX;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.SOFTWARE_HERITAGE_PAGE_SIZE_DEFAULT_VALUE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;

/**
 * Importer module retrieving (incrementally) origins from Software Heritage RESTful endpoint.
 * 
 * @author mhorst
 *
 */
public class SoftwareHeritageOriginsImporter implements eu.dnetlib.iis.common.java.Process {

    private static final String DELIM_LINKS = ",";
    private static final String DELIM_LINK_PARAM = ";";
    private static final String META_REL = "rel";
    private static final String META_NEXT = "next";
    private static final String HEADER_LINK = "Link";

    public static final String DEFAULT_METAFILE_NAME = "meta.json";

    protected static final String COUNTER_NAME_TOTAL = "TOTAL";
    
    protected static final String OUTPUT_PROPERTY_NEXT_RECORD_INDEX = "next_record_index";

    protected static final String PORT_OUT_ORIGINS = "origins";

    private static final Logger log = Logger.getLogger(SoftwareHeritageOriginsImporter.class);

    private final static int progressLogInterval = 100000;

    private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();

    private final Map<String, PortType> outputPorts = new HashMap<String, PortType>();

    // ------------------------ CONSTRUCTORS -------------------

    public SoftwareHeritageOriginsImporter() {
        outputPorts.put(PORT_OUT_ORIGINS, new AvroPortType(SoftwareHeritageOrigin.SCHEMA$));
    }

    // ------------------------ LOGIC --------------------------

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return outputPorts;
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {

        SoftwareHeritageOriginsImporterParams params = new SoftwareHeritageOriginsImporterParams(parameters);
        
        NamedCounters counters = new NamedCounters(new String[] { COUNTER_NAME_TOTAL });
        int currentCount = 0;
        
        if (StringUtils.isNotBlank(params.getShEndpointUriRoot())
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.getShEndpointUriRoot())) {

            try (DataFileWriter<SoftwareHeritageOrigin> originsWriter = getWriter(FileSystem.get(conf), portBindings)) {

                long startTime = System.currentTimeMillis();

                Gson gson = new Gson();

                HttpClient httpclient = buildHttpClient(params.getConnectionTimeout(), params.getReadTimeout());

                HttpHost target = new HttpHost(params.getShEndpointHost(), params.getShEndpointPort(), params.getShEndpointScheme());
                HttpRequest getRequest = new HttpGet(buildUri(params.getShEndpointUriRoot(), params.getStartElementIndex(), params.getPageSize()));

                log.info("executing first page request to " + target + " with: " + getRequest.toString());

                int retryCount=0;
                
                while (getRequest != null) {
                    HttpResponse httpResponse = httpclient.execute(target, getRequest);
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    
                    if (statusCode!=200) {
                        if (statusCode==429) {
                            //got throttled, delaying...
                            log.warn("SH endpoint rate limit reached, delaying for " + params.getDelayMillis()
                                    + " ms, server response: " + EntityUtils.toString(httpResponse.getEntity()));
                            Thread.sleep(params.getDelayMillis());
                            continue;
                        } else {
                            String errMessage = "got unhandled HTTP status code when accessing SH endpoint: "
                                    + statusCode + ", full status: " + httpResponse.getStatusLine()
                                    + ", server response: " + EntityUtils.toString(httpResponse.getEntity());
                            if (retryCount < params.getMaxRetryCount()) {
                                retryCount++;
                                log.error(errMessage + ", number of retries left: " + (params.getMaxRetryCount()-retryCount));
                                Thread.sleep(params.getDelayMillis());
                                continue;
                            } else {
                                throw new RuntimeException(errMessage);    
                            }
                        }
                    } else {
                        if (retryCount > 0) {
                            retryCount=0;    
                        }
                    }

                    HttpEntity entity = httpResponse.getEntity();
                    if (entity != null) {
                        SoftwareHeritageOriginEntry[] entries = parsePage(EntityUtils.toString(entity), gson);
                        if (entries != null && entries.length > 0) {
                            for (SoftwareHeritageOriginEntry entry : entries) {
                                originsWriter.append(convertEntry(entry));
                                counters.increment(COUNTER_NAME_TOTAL);
                                currentCount++;
                                if (currentCount % progressLogInterval == 0) {
                                    log.info("current progress: " + currentCount + ", last package of "
                                            + progressLogInterval + " processed in "
                                            + ((System.currentTimeMillis() - startTime) / 1000) + " secs");
                                    startTime = System.currentTimeMillis();
                                }
                            }
                        }
                    }

                    getRequest = prepareNextRequest(httpResponse);
                }

                log.info("total number of processed records: " + currentCount);
            }
        } else {
            log.warn("no endpoint URI provided, working in offline mode");
        }

        if (counters.currentValue(COUNTER_NAME_TOTAL) == 0) {
            log.warn("no records imported from SH URI: " + params.getShEndpointUriRoot());
        }
        countersWriter.writeCounters(counters, System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
        storeNextElementIndex(params.getStartElementIndex() + currentCount);
    }

    /**
     * Provides {@link SoftwareHeritageOrigin} writer consuming records.
     */
    protected DataFileWriter<SoftwareHeritageOrigin> getWriter(FileSystem fs, PortBindings portBindings)
            throws IOException {
        return DataStore.create(new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_ORIGINS)),
                SoftwareHeritageOrigin.SCHEMA$);
    }

    /**
     * Builds HTTP client issuing requests to SH endpoint.
     */
    protected HttpClient buildHttpClient(int connectionTimeout, int readTimeout) {
        HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout);
        HttpConnectionParams.setSoTimeout(httpParams, readTimeout);
        return new DefaultHttpClient(httpParams);
    }
    
    protected static void storeNextElementIndex(int nextElementIndex) throws IOException {
        File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));

        Properties props = new Properties();
        
        if (file.exists()) {
         // loading properties first, may include counters so we cannot override it
            FileInputStream is = new FileInputStream(file);
            try {
                props.load(is);
            } finally {
                is.close();
            }        
        }
        
        props.put(OUTPUT_PROPERTY_NEXT_RECORD_INDEX, String.valueOf(nextElementIndex));
        
        OutputStream os = new FileOutputStream(file);
        try {
            props.store(os, "");    
        } finally {
            os.close(); 
        }   
    }

    protected static String buildUri(String rootUri, int startElement, int pageSize) {
        StringBuilder strBuilder = new StringBuilder(rootUri);
        strBuilder.append("?origin_from=");
        strBuilder.append(startElement);
        strBuilder.append("&origin_count=");
        strBuilder.append(pageSize);
        return strBuilder.toString();
    }

    protected static SoftwareHeritageOriginEntry[] parsePage(String originsPage, Gson gson) {
        if (StringUtils.isNotBlank(originsPage)) {
            try {
                return gson.fromJson(originsPage, SoftwareHeritageOriginEntry[].class);
            } catch (JsonSyntaxException e) {
                throw new RuntimeException("invalid page contents: \n" + originsPage, e);
            }
        } else {
            return new SoftwareHeritageOriginEntry[0];
        }
    }

    protected static SoftwareHeritageOrigin convertEntry(SoftwareHeritageOriginEntry source) {
        SoftwareHeritageOrigin.Builder resultBuilder = SoftwareHeritageOrigin.newBuilder();
        resultBuilder.setUrl(source.getUrl());
        return resultBuilder.build();
    }

    protected static String getNextLinkFromHeaders(Header[] headers) {
        if (headers != null) {
            for (int i = 0; i < headers.length; i++) {
                if (HEADER_LINK.equals(headers[i].getName())) {
                    return getNextLinkFromHeader(headers[i].getValue());
                }
            }    
        }
        return null;
    }

    protected static String getNextLinkFromHeader(String linkHeader) {
        if (StringUtils.isNotBlank(linkHeader)) {
            String[] links = linkHeader.split(DELIM_LINKS);
            for (String link : links) {
                String[] segments = link.split(DELIM_LINK_PARAM);
                if (segments.length < 2)
                    continue;

                String linkPart = segments[0].trim();
                if (!linkPart.startsWith("<") || !linkPart.endsWith(">")) {
                    continue;
                }
                linkPart = linkPart.substring(1, linkPart.length() - 1);

                for (int i = 1; i < segments.length; i++) {
                    String[] rel = segments[i].trim().split("=");
                    if (rel.length < 2 || !META_REL.equals(rel[0])) {
                        continue;
                    }
                    String relValue = rel[1];
                    if (relValue.startsWith("\"") && relValue.endsWith("\"")) {
                        relValue = relValue.substring(1, relValue.length() - 1);
                    }
                    if (META_NEXT.equals(relValue)) {
                        return linkPart;
                    }

                }
            }
        }

        return null;
    }

    /**
     * Prepares next request based on a link from header. Returns null when next page is not available.
     */
    protected static HttpRequest prepareNextRequest(HttpResponse httpResponse) {
        String nextUrl = getNextLinkFromHeaders(httpResponse.getAllHeaders());
        if (StringUtils.isNotBlank(nextUrl)) {
            return new HttpGet(nextUrl);
        } else {
            return null;
        }
    }

    /**
     * Set of parsed input parameters.
     *
     */
    static class SoftwareHeritageOriginsImporterParams {
        
        private final String shEndpointUriRoot;

        private final String shEndpointHost;
        
        private final String shEndpointScheme;
        
        private final int shEndpointPort;
        
        private final int startElementIndex;
        
        private final int pageSize;
        
        private final int connectionTimeout;
        
        private final int readTimeout;
        
        private final int delayMillis;
        
        private final int maxRetryCount;
        
        public SoftwareHeritageOriginsImporterParams(Map<String, String> parameters) {
            Preconditions.checkArgument(parameters.containsKey(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT),
                    "unknown software heritage endpoint URI, required parameter '%s' is missing!",
                    IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT);
            this.shEndpointUriRoot = parameters.get(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_URI_ROOT);

            Preconditions.checkArgument(parameters.containsKey(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST),
                    "unknown software heritage endpoint host, required parameter '%s' is missing!",
                    IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST);
            this.shEndpointHost = parameters.get(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_HOST);

            Preconditions.checkArgument(parameters.containsKey(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME),
                    "unknown software heritage endpoint scheme (e.g. https), required parameter '%s' is missing!",
                    IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME);
            this.shEndpointScheme = parameters.get(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_SCHEME);

            Preconditions.checkArgument(parameters.containsKey(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT),
                    "unknown software heritage endpoint port, required parameter '%s' is missing!",
                    IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT);
            this.shEndpointPort = Integer.parseInt(parameters.get(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_PORT));

            Preconditions.checkArgument(parameters.containsKey(IMPORT_SOFTWARE_HERITAGE_START_INDEX),
                    "unknown software heritage start element, required parameter '%s' is missing!",
                    IMPORT_SOFTWARE_HERITAGE_START_INDEX);
            this.startElementIndex = Integer.parseInt(WorkflowRuntimeParameters
                    .getParamValueWithUndefinedCheck(IMPORT_SOFTWARE_HERITAGE_START_INDEX, "1", parameters));
            
            this.pageSize = Integer.parseInt(WorkflowRuntimeParameters.getParamValue(IMPORT_SOFTWARE_HERITAGE_PAGE_SIZE,
                    SOFTWARE_HERITAGE_PAGE_SIZE_DEFAULT_VALUE, parameters));

            this.connectionTimeout = Integer.parseInt(WorkflowRuntimeParameters
                    .getParamValue(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_CONNECTION_TIMEOUT, "60000", parameters));
            this.readTimeout = Integer.parseInt(WorkflowRuntimeParameters
                    .getParamValue(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_READ_TIMEOUT, "60000", parameters));
            this.delayMillis = Integer.parseInt(WorkflowRuntimeParameters
                    .getParamValue(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RATELIMIT_DELAY, "10000", parameters));

            this.maxRetryCount = Integer.parseInt(WorkflowRuntimeParameters
                    .getParamValueWithUndefinedCheck(IMPORT_SOFTWARE_HERITAGE_ENDPOINT_RETRY_COUNT, "10", parameters));
        }
        
        public String getShEndpointUriRoot() {
            return shEndpointUriRoot;
        }

        public String getShEndpointHost() {
            return shEndpointHost;
        }

        public String getShEndpointScheme() {
            return shEndpointScheme;
        }

        public int getShEndpointPort() {
            return shEndpointPort;
        }

        public int getStartElementIndex() {
            return startElementIndex;
        }

        public int getPageSize() {
            return pageSize;
        }

        public int getConnectionTimeout() {
            return connectionTimeout;
        }

        public int getReadTimeout() {
            return readTimeout;
        }

        public int getDelayMillis() {
            return delayMillis;
        }

        public int getMaxRetryCount() {
            return maxRetryCount;
        }
        
    }
    
}
