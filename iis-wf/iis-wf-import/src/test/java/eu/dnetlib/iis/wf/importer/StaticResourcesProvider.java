package eu.dnetlib.iis.wf.importer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;


/**
 * Utility class providing static resources.
 * 
 * @author mhorst
 *
 */
public class StaticResourcesProvider {


    //------------------------ CONSTRUCTORS -------------------
    
    private StaticResourcesProvider() {}
    
    //------------------------ LOGIC --------------------------    

    /**
     * Provides all resources for given classpath locations.
     * 
     * @param resourcesLocations resources classpath locations
     */
    public static List<String> getResources(String ...resourcesLocations) {
        List<String> resources = Lists.newArrayList();
        for (String resourceLocation : resourcesLocations) {
            try (InputStream input = StaticResourcesProvider.class.getResourceAsStream(resourceLocation)) {
                resources.add(IOUtils.toString(input));
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource: " + resourceLocation, e);
            }
        }
        return resources;
    }

}
