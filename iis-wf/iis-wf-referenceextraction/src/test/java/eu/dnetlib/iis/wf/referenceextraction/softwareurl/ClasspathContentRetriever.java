package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Classpath based content retriever.
 * Relies on mappings between url and classpath location.
 * 
 * @author mhorst
 *
 */
public class ClasspathContentRetriever implements ContentRetriever {

    private static final long serialVersionUID = -929916697742255820L;
    
    private static final String mappingsLocation = "/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/content-mappings.properties";

    private final Properties urlToClasspathMap;
    
    public ClasspathContentRetriever() throws IOException {
        urlToClasspathMap = new Properties();
        try (InputStream in = ClasspathContentRetriever.class.getResourceAsStream(mappingsLocation)) {
            urlToClasspathMap.load(in);    
        }
    }
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        if (url != null) {
            String classPathLocation = urlToClasspathMap.getProperty(url.toString());
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
            } else {
                return new ContentRetrieverResponse(new DocumentNotFoundException());
            }
        }
        
        return new ContentRetrieverResponse("");
    }

}
