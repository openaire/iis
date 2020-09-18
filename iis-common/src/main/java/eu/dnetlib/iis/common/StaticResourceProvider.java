package eu.dnetlib.iis.common;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StaticResourceProvider {

    /**
     * Returns the path to resource on the classpath.
     *
     * @param location String indicating the location of resource on the classpath.
     * @return Absolute Path to resource.
     */
    public static String getResourcePath(String location) {
        try {
            return new ClassPathResource(location).getURI().getPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the content of a resource on the classpath.
     *
     * @param location String indicating the location of resource on the classpath.
     * @return Content of the resource.
     */
    public static String getResourceContent(String location) {
        try (InputStream input = new ClassPathResource(location).getInputStream()) {
            return IOUtils.toString(input, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a list of contents of resources on the classpath.
     *
     * @param locations Strings indicating the locations of resources on the classpath.
     * @return List of contents of resources.
     */
    public static List<String> getResourcesContents(String... locations) {
        return Arrays.stream(locations)
                .map(StaticResourceProvider::getResourceContent)
                .collect(Collectors.toList());
    }

    /**
     * Returns the input stream of a resource on the classpath.
     *
     * @param location String indicating the location of resource on the classpath.
     * @return InputStream of the resource.
     */
    public static InputStream getResourceInputStream(String location) {
        try {
            return new ClassPathResource(location).getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the input stream reader of a resource on the classpath.
     *
     * @param location String indicating the location of resource on the classpath.
     * @return InputStreamReader of the resource.
     */
    public static InputStreamReader getResourceInputStreamReader(String location) {
        return new InputStreamReader(getResourceInputStream(location), StandardCharsets.UTF_8);
    }
}
