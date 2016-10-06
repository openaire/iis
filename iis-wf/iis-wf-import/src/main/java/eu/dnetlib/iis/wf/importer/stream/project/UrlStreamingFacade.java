package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

/**
 * Straming facade reading compressed data from URL.
 * @author mhorst
 *
 */
public class UrlStreamingFacade implements StreamingFacade {

    private final URL url;
    
    private final boolean compress;
    
    //------------------------ CONSTRUCTOR --------------------------
    
    /**
     * @param endpointLocation stream endpoint URL location
     * @param compress flag indicating stream should be compressed
     * @throws MalformedURLException
     */
    public UrlStreamingFacade(String endpointLocation, boolean compress) throws MalformedURLException {
        this.url = new URL(buildUrl(endpointLocation, compress));
        this.compress = compress;
    }
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public InputStream getStream() throws IOException {
        return this.compress ? new GZIPInputStream(url.openStream()) : url.openStream();
    }

    //------------------------ PRIVATE --------------------------
    
    private static String buildUrl(String endpointLocation, boolean compress) {
        return endpointLocation + "?format=json&compress=" + compress;
    }
    
}
