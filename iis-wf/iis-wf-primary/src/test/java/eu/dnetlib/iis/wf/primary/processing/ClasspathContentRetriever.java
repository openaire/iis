package eu.dnetlib.iis.wf.primary.processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.wf.referenceextraction.softwareurl.ContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.softwareurl.ContentRetrieverResponse;

/**
 * Classpath based content retriever.
 * Relies on mappings between url and classpath location.
 * 
 * @author mhorst
 *
 */
public class ClasspathContentRetriever implements ContentRetriever {

    private static final long serialVersionUID = -929916697742255820L;
    
    private final Map<String,String> urlToClasspathMap;
    
    public ClasspathContentRetriever() throws IOException {
        urlToClasspathMap = new HashMap<>();
        urlToClasspathMap.put("https://github.com/madgik/madis", 
                "/eu/dnetlib/iis/wf/primary/processing/sampledataproducer/input/html/madis.html");

    }
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        if (url != null) {
            String classPathLocation = urlToClasspathMap.get(url.toString());
            if (classPathLocation != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        ClasspathContentRetriever.class.getResourceAsStream(classPathLocation)))) {
                    StringBuilder pageContent = new StringBuilder();
                    String inputLine;
                    while ((inputLine = reader.readLine()) != null) {
                        if (pageContent.length() > 0) {
                            pageContent.append('\n');    
                        }
                        pageContent.append(inputLine);    
                    }
                    return new ContentRetrieverResponse(pageContent.toString());
                } catch (IOException e) {
                    return new ContentRetrieverResponse(e);
                }    
            }
        }
        
        return new ContentRetrieverResponse("");
    }

}
