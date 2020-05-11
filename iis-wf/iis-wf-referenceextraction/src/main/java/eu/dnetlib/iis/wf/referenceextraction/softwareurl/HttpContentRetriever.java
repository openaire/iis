package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;

/**
 * HTTP based content retriever.
 * @author mhorst
 *
 */
public class HttpContentRetriever implements ContentRetriever {


    private static final long serialVersionUID = -6879262115292175343L;
    
    private static final Logger log = Logger.getLogger(HttpContentRetriever.class);

    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        long startTime = System.currentTimeMillis();
        String currentUrl = url.toString();
        
        try {
            log.info("starting content retrieval for url: " + currentUrl);
            
            HttpURLConnection conn = (HttpURLConnection) new URL(currentUrl).openConnection();
            conn.setReadTimeout(readTimeout);
            conn.setConnectTimeout(connectionTimeout);
            
            int status = conn.getResponseCode();
            
            if (status == HttpURLConnection.HTTP_MOVED_TEMP
                || status == HttpURLConnection.HTTP_MOVED_PERM
                    || status == HttpURLConnection.HTTP_SEE_OTHER) {
                currentUrl = conn.getHeaderField("Location");
                log.info("redirecting to: " + currentUrl);
                conn.disconnect();
                conn = (HttpURLConnection) new URL(currentUrl).openConnection();    
            }
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder pageContent = new StringBuilder();
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    if (pageContent.length() < maxPageContentLength) {
                        if (pageContent.length() > 0) {
                            pageContent.append('\n');    
                        }
                        pageContent.append(inputLine);    
                    } else {
                        log.warn("page content from URL: " + currentUrl + " exceeded maximum page length limit: " + maxPageContentLength + ", returning truncated page content");
                        return new ContentRetrieverResponse(pageContent.toString());
                    }
                }
                return new ContentRetrieverResponse(pageContent.toString());
                
            } finally {
                log.info("finished content retrieval for url: " + currentUrl + " in " +
                        (System.currentTimeMillis()-startTime) + " ms");
            }

        } catch (Exception e) {
            log.error("content retrieval failed for url: " + currentUrl, e);
            return new ContentRetrieverResponse(e);
        }
    }

}
