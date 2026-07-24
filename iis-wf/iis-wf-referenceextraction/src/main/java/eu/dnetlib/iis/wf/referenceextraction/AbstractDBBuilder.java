package eu.dnetlib.iis.wf.referenceextraction;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonStreamWriter;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Abstract module building database by executing external process.
 * 
 * @author Dominika Tkaczyk
 * @author mhorst
 *
 */
public abstract class AbstractDBBuilder<T extends SpecificRecord> implements eu.dnetlib.iis.common.java.Process {

    /**
     * Madis script location.
     */
    public final static String PARAM_SCRIPT_LOCATION = "scriptLocation";

    /**
     * Pre-initialized SQLite database location.
     */
    public final static String PARAM_INIT_DB_LOCATION = "initDbLocation";

    /**
     * Input port name.
     */
    public static final String INPUT_PORT = "input";

    /**
     * Output port name pointing to produced database.
     */
    public static final String OUTPUT_PORT = "outputDb";

    /**
     * Avro input records schema.
     */
    private final Schema inputSchema;

    /**
     * Underlying file system facade factory.
     */
    private final FileSystemFacadeFactory fsFacadeFactory;
    
    /**
     * Class encapsulating execution environment.
     *
     */
    public class ProcessExecutionContext {

        private final Process process;
        private final File outputFile;

        /**
         * @param process process to be executed
         * @param targetFile output file to be produced
         */
        public ProcessExecutionContext(Process process, File outputFile) {
            this.process = process;
            this.outputFile = outputFile;
        }

        public Process getProcess() {
            return process;
        }

        public File getOutputFile() {
            return outputFile;
        }
    }

    // -------------------------- CONSTRUCTORS ------------------------------

    /**
     * @param inputSchema avro input records schema
     */
    protected AbstractDBBuilder(Schema inputSchema) {
        this((conf) -> {
            return new HadoopFileSystemFacade(FileSystem.get(conf));
        }, inputSchema);
    }
    
    /**
     * @param fsFacadeFactory file system facade factory
     * @param inputSchema avro input records schema
     */
    protected AbstractDBBuilder(FileSystemFacadeFactory fsFacadeFactory,
            Schema inputSchema) {
        this.fsFacadeFactory = fsFacadeFactory;
        this.inputSchema = inputSchema;
    }

    // -------------------------- LOGIC -------------------------------------

    /**
     * Initializes process generating data on output port based on avro records provided at input.
     * 
     * @param parameters process execution parameters
     * @throws IOException
     */
    protected ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
        String scriptLocation = parameters.get(PARAM_SCRIPT_LOCATION);
        Preconditions.checkArgument(StringUtils.isNotBlank(scriptLocation),
                "sql script location not provided, '%s' parameter is missing!", PARAM_SCRIPT_LOCATION);
        
        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "temp.db";
        File targetDbFile = new File(targetDbLocation);
        
        String initDbLocation = parameters.get(PARAM_INIT_DB_LOCATION);
        if (StringUtils.isNotBlank(initDbLocation) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(initDbLocation)) {
            FileUtils.copyFile(new File(initDbLocation), targetDbFile);
            targetDbFile.setWritable(true);
        }

        return new ProcessExecutionContext(
                Runtime.getRuntime().exec("python /opt/madis/mexec.py -d " + targetDbLocation + " -f " + scriptLocation),
                targetDbFile);
    }
    
    /**
     * Provides input records interator.
     * To be reimplemented by subclasses when needed. 
     *  
     */
    protected CloseableIterator<T> getInputRecordsIterator(FileSystemPath fileSystemPath) throws IOException {
        return DataStore.getReader(fileSystemPath);
    }

    /**
     * Creates output file by reading avro records from input port.
     * 
     */
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters)
            throws IOException, InterruptedException {

        ProcessExecutionContext executionContext = initializeProcess(parameters);
        java.lang.Process process = executionContext.getProcess();

        FileSystemFacade fileSystemFacade = fsFacadeFactory.create(conf);

        try (CloseableIterator<T> inputRecordsIt = getInputRecordsIterator(
                new FileSystemPath(fileSystemFacade.getFileSystem(), portBindings.getInput().get(INPUT_PORT)))) {
            try (JsonStreamWriter<T> writer = new JsonStreamWriter<T>(inputSchema,
                    new BufferedOutputStream(process.getOutputStream()))) {
                while (inputRecordsIt.hasNext()) {
                    writer.write(inputRecordsIt.next());
                }
            }
            process.waitFor();
        } catch (Exception e) {
            throw new IOException("got error while writing to Madis stream: " + getErrorMessage(process.getErrorStream()), e);
        }

        if (process.exitValue() != 0) {
            throw new RuntimeException("MadIS execution failed with error: " + getErrorMessage(process.getErrorStream()));
        }

        try (InputStream inStream = new FileInputStream(executionContext.getOutputFile());
                OutputStream outStream = fileSystemFacade.create(
                        new FileSystemPath(fileSystemFacade.getFileSystem(), portBindings.getOutput().get(OUTPUT_PORT)).getPath())) {
            IOUtils.copy(inStream, outStream);
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        Map<String, PortType> inputPorts = new HashMap<String, PortType>();
        inputPorts.put(INPUT_PORT, new AvroPortType(inputSchema));
        return inputPorts;
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        Map<String, PortType> outputPorts = new HashMap<String, PortType>();
        outputPorts.put(OUTPUT_PORT, new AnyPortType());
        return outputPorts;
    }

    
    // -------------------------- PRIVATE -------------------------------------

    /**
     * Provides error message from error stream.
     */
    private static String getErrorMessage(InputStream errorStream) throws UnsupportedEncodingException, IOException {
        StringBuilder errorBuilder = new StringBuilder();
        try (BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream, "utf8"))) {
            String line;
            while ((line = stderr.readLine()) != null) {
                errorBuilder.append(line);
            }
        }
        return errorBuilder.toString();
    }
    
}
