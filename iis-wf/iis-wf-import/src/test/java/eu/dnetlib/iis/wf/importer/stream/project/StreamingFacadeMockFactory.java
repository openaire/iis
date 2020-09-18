package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import eu.dnetlib.iis.common.StaticResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory providing stream from classpath resource.
 * @author mhorst
 *
 */
public class StreamingFacadeMockFactory implements ServiceFacadeFactory<StreamingFacade> {

    private static String resourceLocation = "/eu/dnetlib/iis/wf/importer/stream/project/data/input/project.json";
    
    @Override
    public StreamingFacade instantiate(Map<String, String> parameters) {

        return new StreamingFacade() {
        
            @Override
            public InputStream getStream() throws IOException {
                return StaticResourceProvider.getResourceInputStream(resourceLocation);
            }
        };
    }

}
