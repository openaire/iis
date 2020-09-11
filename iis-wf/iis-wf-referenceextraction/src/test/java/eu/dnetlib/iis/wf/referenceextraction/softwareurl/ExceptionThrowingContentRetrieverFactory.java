package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory class building {@link ExceptionThrowingContentRetriever}.
 * 
 * @author mhorst
 *
 */
public class ExceptionThrowingContentRetrieverFactory implements ServiceFacadeFactory<ContentRetriever> {

    @Override
    public ContentRetriever instantiate(Map<String, String> parameters) {
        return new ExceptionThrowingContentRetriever();
    }

}
