package eu.dnetlib.iis.common.java.jsonworkflownodes;

import eu.dnetlib.iis.common.java.PortBindings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClassPathResourceToHdfsCopierTest {

    @Mock
    private Function<String, InputStream> classPathResourceProvider;

    private Configuration conf = new Configuration();

    @InjectMocks
    private ClassPathResourceToHdfsCopier classPathResourceToHdfsCopier = new ClassPathResourceToHdfsCopier();

    @Test(expected = NullPointerException.class)
    public void givenParametersWithoutInputClassPathResource_whenRunIsCalled_thenCheckFails() throws Exception {
        classPathResourceToHdfsCopier.run(mock(PortBindings.class), conf, new HashMap<>());
    }

    @Test(expected = NullPointerException.class)
    public void givenParametersWithoutOutputHdfsFileLocation_whenRunIsCalled_thenCheckFails() throws Exception {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("inputClasspathResource", "/path/to/resource");

        classPathResourceToHdfsCopier.run(mock(PortBindings.class), conf, parameters);
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