package eu.dnetlib.iis.common.java.jsonworkflownodes;

import eu.dnetlib.iis.common.java.PortBindings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ClassPathResourceToHdfsCopierTest {

    @Mock
    private Function<String, InputStream> classPathResourceProvider;

    private Configuration conf = new Configuration();

    @InjectMocks
    private ClassPathResourceToHdfsCopier classPathResourceToHdfsCopier = new ClassPathResourceToHdfsCopier();

    @Test
    public void givenParametersWithoutInputClassPathResource_whenRunIsCalled_thenCheckFails() {
        assertThrows(NullPointerException.class, () ->
                classPathResourceToHdfsCopier.run(mock(PortBindings.class), conf, new HashMap<>()));
    }

    @Test
    public void givenParametersWithoutOutputHdfsFileLocation_whenRunIsCalled_thenCheckFails() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("inputClasspathResource", "/path/to/resource");

        assertThrows(NullPointerException.class, () ->
                classPathResourceToHdfsCopier.run(mock(PortBindings.class), conf, parameters));
    }

    @Test
    public void givenParametersWithValues_whenRunIsCalled_thenResourceIsCopiedToLocation() throws Exception {
        Path tempDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
        Path file = tempDirectory.resolve("file.tmp");

        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("inputClasspathResource", "/path/to/resource");
        parameters.put("outputHdfsFileLocation", file.toString());

        when(classPathResourceProvider.apply("/path/to/resource")).thenReturn(
                new ByteArrayInputStream("resource content".getBytes(StandardCharsets.UTF_8)));

        classPathResourceToHdfsCopier.run(mock(PortBindings.class), conf, parameters);

        assertTrue(Files.exists(file));
        assertEquals("resource content", FileUtils.readFileToString(file.toFile(), StandardCharsets.UTF_8));
    }
}