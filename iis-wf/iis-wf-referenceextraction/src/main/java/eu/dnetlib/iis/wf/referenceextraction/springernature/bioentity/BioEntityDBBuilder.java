package eu.dnetlib.iis.wf.referenceextraction.springernature.bioentity;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.wf.referenceextraction.FileSystemFacade;
import eu.dnetlib.iis.wf.referenceextraction.FileSystemFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.HadoopFileSystemFacade;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: add javadoc
public class BioEntityDBBuilder implements Process {

    private final static String PARAM_SCRIPT_LOCATION = "scriptLocation";

    /**
     * Output port name pointing to produced database.
     */
    private final String outputPort;

    /**
     * Underlying file system facade factory.
     */
    private final FileSystemFacadeFactory fsFacadeFactory;

    public BioEntityDBBuilder() {
        this((conf) -> new HadoopFileSystemFacade(FileSystem.get(conf)), "bioentity_db");
    }

    public BioEntityDBBuilder(FileSystemFacadeFactory fsFacadeFactory, String outputPort) {
        this.fsFacadeFactory = fsFacadeFactory;
        this.outputPort = outputPort;
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        String scriptLocation = parameters.get(PARAM_SCRIPT_LOCATION);
        Preconditions.checkArgument(StringUtils.isNotBlank(scriptLocation),
                "sql script location not provided, '%s' parameter is missing!", PARAM_SCRIPT_LOCATION);

        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "bioentities.db";
        File targetDbFile = new File(targetDbLocation);
        targetDbFile.setWritable(true);

        java.lang.Process process = Runtime.getRuntime()
                .exec("python scripts/madis/mexec.py -w " + targetDbLocation + " -f " + scriptLocation);
        process.waitFor();

        if (process.exitValue() != 0) {
            throw new RuntimeException("MadIS execution failed with error: " + getErrorMessage(process.getErrorStream()));
        }

        FileSystemFacade fileSystemFacade = fsFacadeFactory.create(conf);

        try (InputStream inStream = new FileInputStream(targetDbFile);
             OutputStream outStream = fileSystemFacade.create(
                     new FileSystemPath(fileSystemFacade.getFileSystem(), portBindings.getOutput().get(outputPort)).getPath())) {
            IOUtils.copy(inStream, outStream);
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.singletonMap(outputPort, new AnyPortType());
    }

    /**
     * Provides error message from error stream.
     */
    private static String getErrorMessage(InputStream errorStream) throws IOException {
        try (BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8))) {
            return stderr.lines().collect(Collectors.joining());
        }
    }
}
