package eu.dnetlib.iis.wf.export.actionmanager.common;

import eu.dnetlib.iis.common.utils.ListTestUtils;
import eu.dnetlib.iis.wf.export.actionmanager.RDDTestUtils;
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
        conf.setMaster("local[1]");
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
        sc.close();
    }

    @Test
    public void savePairRDDShouldSavePairRDDAsSeqFiles() throws IOException {
        //given
        List<Tuple2<String, String>> tuples = Arrays.asList(
                new Tuple2<>("1L", "1R"),
                new Tuple2<>("2L", "2R"),
                new Tuple2<>("3L", "3R")
        );
        JavaPairRDD<Text, Text> in = sc.parallelize(tuples, NUMBER_OF_OUTPUT_FILES + 1)
                .mapToPair(x -> x.copy(new Text(x._1), new Text(x._2)));

        //when
        RDDUtils.saveTextPairRDD(in, NUMBER_OF_OUTPUT_FILES, outputDir.toString(), configuration);

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = Files.list(outputDir)
                .filter(x -> pattern.matcher(x.getFileName().toString()).matches())
                .count();
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = RDDTestUtils.readValues(outputDir.toString(), Function.identity());
        ListTestUtils
                .compareLists(
                        tuples.stream().map(x -> x._2).sorted().collect(Collectors.toList()),
                        out.stream().map(Text::toString).sorted().collect(Collectors.toList()));
    }
}