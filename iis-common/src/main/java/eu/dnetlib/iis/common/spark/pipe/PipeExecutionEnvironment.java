package eu.dnetlib.iis.common.spark.pipe;

import java.io.IOException;

/**
 * Abstraction of execution environment for scripts and commands run using 'pipe' method on RDD.
 * <p>
 * Classes implementing this interface should propagate any necessary files and directories to cluster nodes and define
 * the command to be run by 'pipe' method. This interface allows to separate production code and test code by providing
 * different implementation for each execution environment.
 */
public interface PipeExecutionEnvironment {

    /**
     * Returns the string command to be run by RDD 'pipe' method. The command executes in the context of each node's
     * local worker dir. Must not reference files or dir by absolute paths. Instead a relative path within worker dir
     * should be used or explicit file path retrieval using 'SparkFiles.get' method.
     *
     * @return String representing the command to be run by RDD 'pipe' method.
     * @throws IOException
     */
    String pipeCommand() throws IOException;
}
