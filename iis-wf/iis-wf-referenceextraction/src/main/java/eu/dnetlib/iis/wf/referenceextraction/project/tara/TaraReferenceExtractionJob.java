package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

import static eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils.clearOutput;
import static eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils.storeInOutput;
import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionUtils.buildDocumentMetadata;
import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionUtils.runReferenceExtraction;

public class TaraReferenceExtractionJob {

    public static final StructType DOCUMENT_METADATA_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("title", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("abstract", DataTypes.StringType, true, Metadata.empty()),
                    StructField$.MODULE$.apply("text", DataTypes.StringType, false, Metadata.empty())
            )
    );

    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            clearOutput(spark, params.outputDocumentToProject);

            AvroDataFrameReader avroDataFrameReader = new AvroDataFrameReader(spark);

            Dataset<Row> inputExtractedDocumentMetadataMergedWithOriginalDF = avroDataFrameReader.read(
                    params.inputExtractedDocumentMetadataMergedWithOriginal,
                    ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$);
            Dataset<Row> inputDocumentTextDF = avroDataFrameReader.read(params.inputDocumentText,
                    DocumentText.SCHEMA$);
            Dataset<Row> documentMetadataDF = buildDocumentMetadata(inputDocumentTextDF,
                    inputExtractedDocumentMetadataMergedWithOriginalDF);

            TaraPipeExecutionEnvironment environment = new TaraPipeExecutionEnvironment(spark.sparkContext(),
                    params.scriptsDir, params.projectDbFile);
            Dataset<Row> documentToProjectDF = runReferenceExtraction(spark,
                    documentMetadataDF,
                    environment);

            storeInOutput(spark,
                    documentToProjectDF,
                    params.outputDocumentToProject);
        }
    }

    @Parameters(separators = "=")
    public static class JobParameters {

        @Parameter(names = "-inputExtractedDocumentMetadataMergedWithOriginal", required = true)
        private String inputExtractedDocumentMetadataMergedWithOriginal;

        @Parameter(names = "-inputDocumentText", required = true)
        private String inputDocumentText;

        @Parameter(names = "-scriptsDir", required = true)
        private String scriptsDir;

        @Parameter(names = "-projectDbFile", required = true)
        private String projectDbFile;

        @Parameter(names = "-outputDocumentToProject", required = true)
        private String outputDocumentToProject;
    }
}
