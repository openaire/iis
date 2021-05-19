package eu.dnetlib.iis.wf.importer.elsevier;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.elsevier.spark_xml_utils.xpath.XPathProcessor;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

/**
 * Creates a {@link DocumentText} avro data store for Elsevier archives.
 * <p>
 * The job reads Elsevier zip archives and creates an avro data store with file as id and extracted acknowledge
 * statement as text.
 */
public class ImportElsevierXmlContentJob {

    public static void main(String[] args) {
        JobParams params = new JobParams();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        runWithSparkSession(new SparkConf(), params.isSparkSessionShared, spark -> {

            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputPath);

            JavaRDD<Row> rdd = spark.sparkContext().binaryFiles(params.inputPath, spark.sparkContext().defaultMinPartitions()).toJavaRDD()
                    .flatMap(pair -> {
                        String zipFileName = pair._1();
                        ZipInputStream zis = new ZipInputStream(pair._2().open());

                        List<Row> rows = new ArrayList<>();
                        ZipEntry zipEntry;
                        while ((zipEntry = zis.getNextEntry()) != null) {
                            String fileName = zipEntry.getName();
                            BufferedReader reader = new BufferedReader(new InputStreamReader(zis));

                            StringBuilder content = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                content.append(line);
                            }
                            rows.add(RowFactory.create(String.format("%s/%s", zipFileName, fileName), content.toString()));
                        }

                        return rows.iterator();
                    });

            StructType schema = StructType$.MODULE$.apply(
                    Arrays.asList(
                            StructField$.MODULE$.apply("id", DataTypes.StringType, true, Metadata.empty()),
                            StructField$.MODULE$.apply("document", DataTypes.StringType, true, Metadata.empty())
                    )
            );
            Dataset<Row> df = spark.createDataFrame(rdd, schema);

            Dataset<Row> idAndTextDF = df
                    .select(col("id"),
                            get_xml_object_string("/*:document/*:article/*:body/*:acknowledgment/string()").apply(col("document")).as("text"))
                    .where(col("text").isNotNull().and(col("text").notEqual("")));

            Dataset<Row> documentTextDF = spark.createDataFrame(idAndTextDF.javaRDD(),
                    (StructType) SchemaConverters.toSqlType(DocumentText.SCHEMA$).dataType());

            new AvroDataFrameWriter(documentTextDF).write(params.outputPath, DocumentText.SCHEMA$);
        });
    }

    public static UserDefinedFunction get_xml_object_string(String xPath) {
        return udf((UDF1<String, String>) xml -> {
            XPathProcessor proc = XPathProcessor.getInstance(xPath);
            return proc.evaluate(xml);
        }, DataTypes.StringType);
    }

    @Parameters(separators = "=")
    public static class JobParams {

        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;

        @Parameter(names = "-inputPath", required = true)
        private String inputPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
