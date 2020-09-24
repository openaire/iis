package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Utility class responsible for copying resources available on classpath to specified HDFS location.
 * @author mhorst
 *
 */
public class ClassPathResourceToHdfsCopier implements Process {

    private static final String PARAM_INPUT_CLASSPATH_RESOURCE = "inputClasspathResource";
    
    private static final String PARAM_OUTPUT_HDFS_FILE_LOCATION = "outputHdfsFileLocation";

    private Function<String, InputStream> classPathResourceProvider = ClassPathResourceProvider::getResourceInputStream;

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        Preconditions.checkNotNull(parameters.get(PARAM_INPUT_CLASSPATH_RESOURCE), PARAM_INPUT_CLASSPATH_RESOURCE + " parameter was not specified!");
        Preconditions.checkNotNull(parameters.get(PARAM_OUTPUT_HDFS_FILE_LOCATION), PARAM_OUTPUT_HDFS_FILE_LOCATION + " parameter was not specified!");

        FileSystem fs = FileSystem.get(conf);

        try (InputStream in = classPathResourceProvider.apply(parameters.get(PARAM_INPUT_CLASSPATH_RESOURCE));
             OutputStream os = fs.create(new Path(parameters.get(PARAM_OUTPUT_HDFS_FILE_LOCATION)))) {
            IOUtils.copyBytes(in, os, 4096, false);
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return new HashMap<>();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return new HashMap<>();
    }

}
