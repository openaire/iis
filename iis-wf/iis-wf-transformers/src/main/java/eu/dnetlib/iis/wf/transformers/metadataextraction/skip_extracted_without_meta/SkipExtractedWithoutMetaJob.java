package eu.dnetlib.iis.wf.transformers.metadataextraction.skip_extracted_without_meta;

import java.io.IOException;
import java.util.Optional;

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
 *
 * Replaces the PIG-based skip_extracted_without_meta transformer workflow step.
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

            // Collect the set of already-extracted document ids
            JavaPairRDD<String, ExtractedDocumentMetadata> metaById = documentMeta.mapToPair(
                    m -> new Tuple2<>(m.getId().toString(), m));

            JavaPairRDD<String, DocumentContentUrl> contentById = documentContent.mapToPair(
                    d -> new Tuple2<>(d.getId().toString(), d));

            // Left join content with meta; keep only records where meta is absent
            JavaRDD<DocumentContentUrl> output = contentById
                    .leftOuterJoin(metaById)
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
