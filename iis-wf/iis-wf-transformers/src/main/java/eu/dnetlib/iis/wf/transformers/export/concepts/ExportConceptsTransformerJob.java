package eu.dnetlib.iis.wf.transformers.export.concepts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that groups DocumentToConceptId records by documentId and produces a
 * DocumentToConceptIds export record per document. When the same conceptId appears
 * multiple times, only the entry with the highest confidenceLevel is kept.
 */
public class ExportConceptsTransformerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<DocumentToConceptId> input = avroLoader.loadJavaRDD(sc, params.input,
                    DocumentToConceptId.class);

            JavaPairRDD<String, Iterable<DocumentToConceptId>> grouped = input
                    .mapToPair(c -> new Tuple2<>(c.getDocumentId().toString(), c))
                    .groupByKey();

            JavaRDD<DocumentToConceptIds> output = grouped.map(pair -> {
                // Deduplicate by conceptId, keeping entry with highest confidenceLevel.
                // TreeMap gives lexicographic order, matching IdConfidenceTupleDeduplicator behaviour.
                Map<String, Float> conceptMap = new TreeMap<>();
                for (DocumentToConceptId c : pair._2) {
                    String cid = c.getConceptId().toString();
                    float cl = c.getConfidenceLevel();
                    if (!conceptMap.containsKey(cid) || cl > conceptMap.get(cid)) {
                        conceptMap.put(cid, cl);
                    }
                }
                List<Concept> concepts = new ArrayList<>();
                for (Map.Entry<String, Float> entry : conceptMap.entrySet()) {
                    concepts.add(Concept.newBuilder()
                            .setId(entry.getKey())
                            .setConfidenceLevel(entry.getValue())
                            .build());
                }
                return DocumentToConceptIds.newBuilder()
                        .setDocumentId(pair._1)
                        .setConcepts(concepts)
                        .build();
            });

            avroSaver.saveJavaRDD(output, DocumentToConceptIds.SCHEMA$, params.output);
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
