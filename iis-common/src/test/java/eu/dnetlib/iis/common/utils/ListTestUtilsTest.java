package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SlowTest
public class ListTestUtilsTest {
    private static JavaSparkContext sc;
    private static Configuration configuration;

    @TempDir
    public Path workingDir;

    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(ListTestUtilsTest.class.getSimpleName());
        sc = JavaSparkContextFactory.withConfAndKryo(conf);
        configuration = new Configuration();
    }

    @AfterAll
    public static void afterAll() {
        sc.stop();
    }

    @Test
    public void compareShouldThrowExceptionWhenListsNotMatch() {
        //given
        List<String> left = Arrays.asList("a", "b");
        List<String> right = Arrays.asList("a", "x");

        //when
        assertThrows(AssertionFailedError.class, () -> ListTestUtils.compareLists(left, right));
    }

    @Test
    public void compareShouldNotThrowExceptionWhenListsMatch() {
        //given
        List<String> left = Arrays.asList("a", "b");
        List<String> right = Arrays.asList("a", "b");

        //when
        ListTestUtils.compareLists(left, right);
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
        Path inputDir = workingDir.resolve("input");
        RDDUtils.saveTextPairRDD(pairs, 2, inputDir.toString(), configuration);

        //when
        List<Text> values = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(inputDir.toString()));

        //then
        ListTestUtils
                .compareLists(
                        tuples.stream().map(x -> x._2).sorted().collect(Collectors.toList()),
                        values.stream().map(Text::toString).sorted().collect(Collectors.toList())
                );
    }
}
