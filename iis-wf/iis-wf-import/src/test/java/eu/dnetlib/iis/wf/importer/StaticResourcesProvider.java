package eu.dnetlib.iis.wf.importer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.beust.jcommander.internal.Lists;

/**
 * Encapsulates list of resources created from classpath locations.
 * 
 * @author mhorst
 *
 */
public class StaticResourcesProvider {

    /**
     * List of resources to be provided.
     */
    private final List<String> resources;
    
    //------------------------ CONSTRUCTORS --------------------------
    

    /**
     * @param resourcesLocations resources to be read classpath locations
     */
    public StaticResourcesProvider(String ...resourcesLocations) {
        resources = Lists.newArrayList(resourcesLocations.length);
        for (String resourceLocation : resourcesLocations) {
            try (InputStream input = getClass().getResourceAsStream(resourceLocation)) {
                resources.add(IOUtils.toString(input));
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource: " + resourceLocation, e);
            }
        }
    }
    
    //------------------------ GETTERS --------------------------    

    /**
     * Provides all resources.
     */
    public List<String> getResources() {
        return resources;
    }

}
