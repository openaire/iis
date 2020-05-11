package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Job responsible for extracting {@link Patent} metadata out of the XML file
 * obtained from EPO endpoint.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataExtractorJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    // ------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);

            JavaRDD<Patent> parsedPatents = avroLoader.loadJavaRDD(sc, params.inputPath, DocumentText.class)
                    .map(PatentMetadataExtractorJob::convert);

            avroSaver.saveJavaRDD(parsedPatents, Patent.SCHEMA$, params.outputPath);
        }
    }

    // ------------------------ PRIVATE --------------------------

    private static Patent convert(DocumentText patent) {
        // FIXME implement me
        throw new RuntimeException("Not Implemented Yet!");
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
