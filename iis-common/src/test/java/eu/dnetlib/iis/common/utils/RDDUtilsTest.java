package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDUtilsTest {
    private static final int NUMBER_OF_OUTPUT_FILES = 2;
    private static JavaSparkContext sc;
    private static Configuration configuration;

    @TempDir
    public Path workingDir;

    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(RDDUtilsTest.class.getSimpleName());
        sc = JavaSparkContextFactory.withConfAndKryo(conf);
        configuration = new Configuration();
    }

    @AfterAll
    public static void afterAll() {
        sc.stop();
    }

    @Test
    @DisplayName("Pair RDD is saved with specified number of files")
    public void savePairRDDShouldSavePairRDDAsSeqFilesWithSpecifiedNumberOfFiles() throws IOException {
        //given
        JavaPairRDD<Text, Text> in = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("1L"), new Text("1R")),
                new Tuple2<>(new Text("2L"), new Text("2R")),
                new Tuple2<>(new Text("3L"), new Text("3R"))),
                NUMBER_OF_OUTPUT_FILES + 1);
        Path outputDir = workingDir.resolve("output");

        //when
        RDDUtils.saveTextPairRDD(in, NUMBER_OF_OUTPUT_FILES, outputDir.toString(), configuration);

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = HdfsUtils.countFiles(new Configuration(), outputDir.toString(), x ->
                pattern.matcher(x.getName()).matches());
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = ListTestUtils.readValues(outputDir.toString(), Function.identity());
        List<String> actualValues = out.stream().map(Text::toString).sorted().collect(Collectors.toList());
        List<String> expectedValues = in.collect().stream().map(x -> x._2.toString()).sorted().collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualValues, expectedValues);
    }

    @Test
    @DisplayName("Pair RDD is saved")
    public void savePairRDDShouldSavePairRDDAsSeqFiles() throws IOException {
        //given
        JavaPairRDD<Text, Text> in = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("1L"), new Text("1R")),
                new Tuple2<>(new Text("2L"), new Text("2R")),
                new Tuple2<>(new Text("3L"), new Text("3R"))),
                NUMBER_OF_OUTPUT_FILES);
        Path outputDir = workingDir.resolve("output");

        //when
        RDDUtils.saveTextPairRDD(in, outputDir.toString(), configuration);

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = HdfsUtils.countFiles(new Configuration(), outputDir.toString(), x ->
                pattern.matcher(x.getName()).matches());
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = ListTestUtils.readValues(outputDir.toString(), Function.identity());
        List<String> actualValues = out.stream().map(Text::toString).sorted().collect(Collectors.toList());
        List<String> expectedValues = in.collect().stream().map(x -> x._2.toString()).sorted().collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualValues, expectedValues);
    }
}
