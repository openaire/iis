package eu.dnetlib.iis.common.spark.pipe;

import java.io.IOException;

public interface PipeExecutionEnvironment {
    String pipeCommand() throws IOException;
}
