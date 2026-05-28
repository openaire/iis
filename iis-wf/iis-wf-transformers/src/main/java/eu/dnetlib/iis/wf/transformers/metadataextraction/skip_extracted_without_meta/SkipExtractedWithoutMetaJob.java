package eu.dnetlib.iis.wf.transformers.metadataextraction.skip_extracted_without_meta;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that filters out DocumentContentUrl records whose id is already present in the
 * ExtractedDocumentMetadata input (i.e. already processed). Only records not yet extracted
 * are written to the output.
 */
public class SkipExtractedWithoutMetaJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputDocumentContent);

            JavaRDD<DocumentContentUrl> documentContent = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentContent, DocumentContentUrl.class);
            JavaRDD<ExtractedDocumentMetadata> documentMeta = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentMeta, ExtractedDocumentMetadata.class);

            JavaPairRDD<String, DocumentContentUrl> contentById = documentContent.mapToPair(
                    d -> new Tuple2<>(d.getId().toString(), d));

            // Distinct set of already-extracted document ids (matching PIG's 'distinct cachedDocumentId').
            // Deduplication avoids a cross-product explosion when meta has duplicate ids, and prevents
            // duplicate content records in the output when both sides share the same key.
            JavaPairRDD<String, Boolean> distinctMetaIds = documentMeta
                    .mapToPair(m -> new Tuple2<>(m.getId().toString(), Boolean.TRUE))
                    .reduceByKey((a, b) -> a);

            // Left join content with distinct meta ids; keep only records where meta is absent
            JavaRDD<DocumentContentUrl> output = contentById
                    .leftOuterJoin(distinctMetaIds)
                    .filter(pair -> !pair._2._2.isPresent())
                    .map(pair -> pair._2._1);

            avroSaver.saveJavaRDD(output, DocumentContentUrl.SCHEMA$, params.outputDocumentContent);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputDocumentContent", required = true)
        private String inputDocumentContent;

        @Parameter(names = "-inputDocumentMeta", required = true)
        private String inputDocumentMeta;

        @Parameter(names = "-outputDocumentContent", required = true)
        private String outputDocumentContent;
    }
}
