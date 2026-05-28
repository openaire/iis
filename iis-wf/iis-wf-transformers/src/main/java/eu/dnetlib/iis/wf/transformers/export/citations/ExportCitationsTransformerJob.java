package eu.dnetlib.iis.wf.transformers.export.citations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.export.schemas.Citations;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that groups Citation records by sourceDocumentId and produces a single
 * Citations export record per document with its citation entries sorted by position.
 */
public class ExportCitationsTransformerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<Citation> input = avroLoader.loadJavaRDD(sc, params.input, Citation.class);

            JavaPairRDD<String, Iterable<Citation>> grouped = input
                    .mapToPair(c -> new Tuple2<>(c.getSourceDocumentId().toString(), c))
                    .groupByKey();

            JavaRDD<Citations> output = grouped.map(pair -> {
                List<CitationEntry> entries = new ArrayList<>();
                for (Citation c : pair._2) {
                    entries.add(c.getEntry());
                }
                entries.sort(Comparator.comparingInt(e -> e.getPosition() != null ? e.getPosition() : 0));
                return Citations.newBuilder()
                        .setDocumentId(pair._1)
                        .setCitations(entries)
                        .build();
            });

            avroSaver.saveJavaRDD(output, Citations.SCHEMA$, params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
