package eu.dnetlib.iis.wf.primary.processing;

import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.softwareurl.ContentRetriever;

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
    
    public ClasspathContentRetriever() {
        urlToClasspathMap = new HashMap<>();
        urlToClasspathMap.put("https://github.com/madgik/madis", 
                "/eu/dnetlib/iis/wf/primary/processing/sampledataproducer/input/html/madis.html");

    }
    
    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url) {
        if (url != null) {
            String classPathLocation = urlToClasspathMap.get(url.toString());
            if (classPathLocation != null) {
                try {
                    return new ContentRetrieverResponse(ClassPathResourceProvider.getResourceContent(classPathLocation));
                } catch (Exception e) {
                    return new ContentRetrieverResponse(e);
                }
            }
        }
        
        return new ContentRetrieverResponse("");
    }

}
