package eu.dnetlib.iis.wf.referenceextraction.covid19;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.MatchedDocument;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

/**
 * Covid-19 reference extraction job.
 * <br/><br/>
 * Processes documents {@link ExtractedDocumentMetadataMergedWithOriginal} and calculates
 * {@link eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId} out of them.
 *
 * @author Marek Horst
 */
public class Covid19ReferenceExtractionJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static final String COUNTER_REFERENCES_TOTAL = "processing.referenceExtraction.covid-19.reference.total";

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Covid19ReferenceExtractionJobParameters params = new Covid19ReferenceExtractionJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputReportPath);

            spark.sparkContext().addFile(params.scriptDirPath, true);

            String conceptId = params.predefinedConceptId;
            float confidenceLevel = Float.parseFloat(params.predefinedConfidenceLevel);

            AvroDataFrameReader avroDataFrameReader = new AvroDataFrameReader(spark);
            Dataset<Row> rawDocuments = avroDataFrameReader.read(
                    params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$);

            Dataset<Row> repartDocuments = shouldRepartition(params.numberOfPartitions)
                    ? rawDocuments.repartition(Integer.parseInt(params.numberOfPartitions))
                    : rawDocuments;

            // Select only the fields required by covid19extract.sql and filter blank abstracts.
            // toJSON() serialises each row as a single-line JSON string consumed by madis via stdinput();
            // the SQL uses jsonpath(c1, '$.id', '$.title', '$.abstract', '$.date') to read these fields.
            Dataset<String> metadataJson = repartDocuments
                    .filter(col("abstract").isNotNull().and(length(col("abstract")).gt(0)))
                    .select(
                            col("id"),
                            col("title"),
                            col("abstract"),
                            col("year").cast(DataTypes.StringType).alias("date"))
                    .toJSON();

            // SparkFiles.get() resolves the absolute path on the executor node in all deployment modes:
            // - local: path as distributed by sc.addFile() from the local filesystem
            // - YARN cluster: path within the YARN container working directory
            // - Kubernetes: path within the Spark executor pod work-dir (e.g. /opt/spark/work-dir/scripts)
            // The scriptDirPath parameter must point to an HDFS directory containing extract_references.sh
            // and covid19extract.sql; sc.addFile() distributes that directory to every executor pod.
            // String scriptsDirOnWorkerNode = SparkFiles.get("scripts");
            String scriptsDirOnWorkerNode = "scripts";

            JavaRDD<String> matchedDocumentsRDD = metadataJson.toJavaRDD()
                    .pipe("bash " + scriptsDirOnWorkerNode + "/extract_references.sh " + scriptsDirOnWorkerNode);

            // Parse each pipe output line (MatchedDocument JSON) and convert to a DocumentToConceptId row.
            // StructType outputSchema = new StructType()
            //        .add("documentId", DataTypes.StringType, false)
            //        .add("conceptId", DataTypes.StringType, false)
            //        .add("confidenceLevel", DataTypes.FloatType, false);
            StructType outputSchema = (StructType) SchemaConverters.toSqlType(DocumentToConceptId.SCHEMA$).dataType();

            JavaRDD<Row> convertedRowsRDD = matchedDocumentsRDD.map(recordString -> {
                MatchedDocument matched = AvroGsonFactory.create().fromJson(recordString, MatchedDocument.class);
                return RowFactory.create(matched.getId().toString(), conceptId, confidenceLevel);
            });

            Dataset<Row> convertedDocumentsDF = spark.createDataFrame(convertedRowsRDD, outputSchema);

            convertedDocumentsDF.cache();

            new ReferenceExtractionIOUtils.AvroDataStoreWriter()
                    .write(convertedDocumentsDF, params.outputAvroPath, DocumentToConceptId.SCHEMA$);

            List<ReportEntry> reportEntries = generateReport(convertedDocumentsDF.count());
            JavaRDD<ReportEntry> reportRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(reportEntries, 1);
            avroSaver.saveJavaRDD(reportRDD, ReportEntry.SCHEMA$, params.outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    /**
     * Checks whether repartitioning should be performed based on the parameter value.
     */
    private static boolean shouldRepartition(String numberOfPartitions) {
        return StringUtils.isNotBlank(numberOfPartitions)
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(numberOfPartitions);
    }

    /**
     * Generates the reference extraction execution report.
     *
     * @param totalResultsCount total number of matched documents
     */
    private static List<ReportEntry> generateReport(long totalResultsCount) {
        return ImmutableList.of(createCounterReportEntry(COUNTER_REFERENCES_TOTAL, totalResultsCount));
    }

    @Parameters(separators = "=")
    private static class Covid19ReferenceExtractionJobParameters {

        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;

        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;

        @Parameter(names = "-predefinedConceptId", required = true)
        private String predefinedConceptId;

        @Parameter(names = "-predefinedConfidenceLevel", required = true)
        private String predefinedConfidenceLevel;

        @Parameter(names = "-scriptDirPath", required = true, description = "path to directory with scripts")
        private String scriptDirPath;

        @Parameter(names = "-numberOfPartitions", required = false, description = "number of partitions the data should be sliced into")
        private String numberOfPartitions;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
