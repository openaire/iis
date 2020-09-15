package eu.dnetlib.iis.common.utils;

import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Common resource utilities methods.
 */
public class ResourceUtils {

    /**
     * Returns a resource path with properly handled special characters.
     *
     * @param cl   ClassLoader instance for resource extraction.
     * @param name Path of the resource
     * @return String representing the path to the resource.
     */
    public static String resourcePath(ClassLoader cl, String name) {
        try {
            return Objects.requireNonNull(cl.getResource(name)).toURI().getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
