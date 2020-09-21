package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;

import java.io.IOException;
import java.io.InputStream;
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
        try (InputStream in = ClassPathResourceProvider.getResourceInputStream(mappingsLocation)) {
            urlToClasspathMap.load(in);
        }
    }

    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url) {
        if (url != null) {
            String classPathLocation = urlToClasspathMap.getProperty(url.toString());
            if (classPathLocation != null) {
                try {
                    return new ContentRetrieverResponse(ClassPathResourceProvider.getResourceContent(classPathLocation));
                } catch (Exception e) {
                    return new ContentRetrieverResponse(e);
                }
            } else {
                return new ContentRetrieverResponse(new DocumentNotFoundException());
            }
        }

        return new ContentRetrieverResponse("");
    }

}
