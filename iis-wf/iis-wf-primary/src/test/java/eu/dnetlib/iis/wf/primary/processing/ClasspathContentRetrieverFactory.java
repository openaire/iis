package eu.dnetlib.iis.wf.primary.processing;

import java.io.IOException;
import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.softwareurl.ContentRetriever;

/**
 * Factory class building {@link ClasspathContentRetriever}.
 * 
 * @author mhorst
 *
 */
public class ClasspathContentRetrieverFactory implements ServiceFacadeFactory<ContentRetriever> {

    @Override
    public ContentRetriever instantiate(Map<String, String> parameters) {
        try {
            return new ClasspathContentRetriever();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
