package eu.dnetlib.iis.common.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RDDUtilsTest {
    private static final int NUMBER_OF_OUTPUT_FILES = 2;
    private static JavaSparkContext sc;
    private static Configuration configuration;

    private Path workingDir;
    private Path outputDir;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.setAppName("RDDUtilsTest");
        sc = new JavaSparkContext(conf);
        configuration = new Configuration();
    }

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("RDDUtilsTest_");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @AfterClass
    public static void afterClass() {
        sc.stop();
    }

    @Test
    public void savePairRDDShouldSavePairRDDAsSeqFiles() throws IOException {
        //given
        JavaPairRDD<Text, Text> in = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("1L"), new Text("1R")),
                new Tuple2<>(new Text("2L"), new Text("2R")),
                new Tuple2<>(new Text("3L"), new Text("3R"))),
                NUMBER_OF_OUTPUT_FILES + 1);

        //when
        RDDUtils.saveTextPairRDD(in, NUMBER_OF_OUTPUT_FILES, outputDir.toString(), configuration);

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = Files.list(outputDir)
                .filter(x -> pattern.matcher(x.getFileName().toString()).matches())
                .count();
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = RDDTestUtils.readValues(outputDir.toString(), Function.identity());
        List<String> actualValues = out.stream().map(Text::toString).sorted().collect(Collectors.toList());
        List<String> expectedValues = in.collect().stream().map(x -> x._2.toString()).sorted().collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualValues, expectedValues);
    }
}
