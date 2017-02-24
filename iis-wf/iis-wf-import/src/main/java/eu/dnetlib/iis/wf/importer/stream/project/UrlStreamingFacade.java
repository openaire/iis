package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

/**
 * Straming facade reading compressed data from URL.
 * @author mhorst
 *
 */
public class UrlStreamingFacade implements StreamingFacade {

    private final Logger log = Logger.getLogger(this.getClass());
    
    private final URL url;
    
    private final boolean compress;
    
    private final int readTimeout;
    
    private final int connectionTimeout;
    
    //------------------------ CONSTRUCTOR --------------------------
    
    /**
     * @param endpointLocation stream endpoint URL location
     * @param compress flag indicating stream should be compressed
     * @param readTimeout url read timeout
     * @param connectionTimeout url connection timeout
     * @throws MalformedURLException
     */
    public UrlStreamingFacade(String endpointLocation, boolean compress,
            int readTimeout, int connectionTimeout) throws MalformedURLException {
        this.url = new URL(buildUrl(endpointLocation, compress));
        this.compress = compress;
        this.readTimeout = readTimeout;
        this.connectionTimeout = connectionTimeout;
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public InputStream getStream() throws IOException {
        return this.compress ? new GZIPInputStream(getStreamWithTimeouts()) : getStreamWithTimeouts();
    }

    //------------------------ PRIVATE --------------------------
    
    private static String buildUrl(String endpointLocation, boolean compress) {
        return endpointLocation + "?format=json&compress=" + compress;
    }
    
    private InputStream getStreamWithTimeouts() throws IOException {
        log.info(String.format("setting timeouts for streaming service: read timeout (%s) and connect timeout (%s)", 
                this.readTimeout, this.connectionTimeout));
        URLConnection con = url.openConnection();
        con.setReadTimeout(this.readTimeout);
        con.setConnectTimeout(this.connectionTimeout);
        return con.getInputStream();
    }
}
