package eu.dnetlib.iis.wf.importer.facade;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

/**
 * Context straming facade reading data from URL.
 * @author mhorst
 *
 */
public class ContextUrlStreamingFacade implements ContextStreamingFacade {

    private static final Logger log = Logger.getLogger(ContextUrlStreamingFacade.class);
    
    private final String endpointLocation;
    
    private final int readTimeout;
    
    private final int connectionTimeout;
    
    
    //------------------------ CONSTRUCTOR --------------------------
    
    /**
     * @param endpointLocation stream endpoint URL location
     * @param readTimeout url read timeout
     * @param connectionTimeout url connection timeout
     * @throws MalformedURLException
     */
    public ContextUrlStreamingFacade(String endpointLocation, 
            int readTimeout, int connectionTimeout) throws MalformedURLException {
        this.endpointLocation = endpointLocation;
        this.readTimeout = readTimeout;
        this.connectionTimeout = connectionTimeout;
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public InputStream getStream(String contextId) throws ContextNotFoundException, ContextStreamingException {
        try {
            log.info(String.format("setting timeouts for streaming service: read timeout (%s) and connect timeout (%s)", 
                    this.readTimeout, this.connectionTimeout));
            URL url = new URL(buildUrl(endpointLocation, contextId));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setReadTimeout(this.readTimeout);
            con.setConnectTimeout(this.connectionTimeout);
            if (HttpURLConnection.HTTP_NOT_FOUND == con.getResponseCode()) {
                throw new ContextNotFoundException(contextId);
            } else {
                return con.getInputStream();    
            }    
        } catch (IOException e) {
            throw new ContextStreamingException(contextId, e);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static String buildUrl(String endpointLocation, String contextId) {
        return endpointLocation + "/" + contextId;
    }
}
