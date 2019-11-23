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
import java.util.stream.Collectors;

public class RDDTestUtilsTest {
    private static JavaSparkContext sc;
    private static Configuration configuration;

    private Path workingDir;
    private Path inputDir;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("RDDTestUtilsTest");
        sc = new JavaSparkContext(conf);
        configuration = new Configuration();
    }

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("RDDTestUtilsTest_");
        inputDir = workingDir.resolve("input");
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
    public void readValuesShouldReadValuesAsTextFromSeqFile() throws IOException {
        //given
        List<Tuple2<String, String>> tuples = Arrays.asList(
                new Tuple2<>("1L", "1R"),
                new Tuple2<>("2L", "2R"),
                new Tuple2<>("3L", "3R")
        );
        JavaPairRDD<Text, Text> pairs = sc.parallelize(tuples)
                .mapToPair(x -> x.copy(new Text(x._1), new Text(x._2)));
        RDDUtils.saveTextPairRDD(pairs, 2, inputDir.toString(), configuration);

        //when
        List<Text> values = RDDTestUtils.readValues(inputDir.toString(), Function.identity());

        //then
        ListTestUtils
                .compareLists(
                        tuples.stream().map(x -> x._2).sorted().collect(Collectors.toList()),
                        values.stream().map(Text::toString).sorted().collect(Collectors.toList())
                );
    }
}
