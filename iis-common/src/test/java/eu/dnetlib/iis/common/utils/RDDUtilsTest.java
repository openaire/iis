package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDUtilsTest extends TestWithSharedSparkContext {
    private static final int NUMBER_OF_OUTPUT_FILES = 2;

    @TempDir
    public Path workingDir;

    @Test
    @DisplayName("Pair RDD is saved with specified number of files")
    public void savePairRDDShouldSavePairRDDAsSeqFilesWithSpecifiedNumberOfFiles() throws IOException {
        //given
        JavaPairRDD<Text, Text> in = jsc().parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("1L"), new Text("1R")),
                new Tuple2<>(new Text("2L"), new Text("2R")),
                new Tuple2<>(new Text("3L"), new Text("3R"))),
                NUMBER_OF_OUTPUT_FILES + 1);
        Path outputDir = workingDir.resolve("output");

        //when
        RDDUtils.saveTextPairRDD(in, NUMBER_OF_OUTPUT_FILES, outputDir.toString(), jsc().hadoopConfiguration());

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = HdfsUtils.countFiles(new Configuration(), outputDir.toString(), x ->
                pattern.matcher(x.getName()).matches());
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputDir.toString()));
        List<String> actualValues = out.stream().map(Text::toString).sorted().collect(Collectors.toList());
        List<String> expectedValues = in.collect().stream().map(x -> x._2.toString()).sorted().collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualValues, expectedValues);
    }

    @Test
    @DisplayName("Pair RDD is saved")
    public void savePairRDDShouldSavePairRDDAsSeqFiles() throws IOException {
        //given
        JavaPairRDD<Text, Text> in = jsc().parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("1L"), new Text("1R")),
                new Tuple2<>(new Text("2L"), new Text("2R")),
                new Tuple2<>(new Text("3L"), new Text("3R"))),
                NUMBER_OF_OUTPUT_FILES);
        Path outputDir = workingDir.resolve("output");

        //when
        RDDUtils.saveTextPairRDD(in, outputDir.toString(), jsc().hadoopConfiguration());

        //then
        Pattern pattern = Pattern.compile("^part-r-.\\d+$");
        long fileCount = HdfsUtils.countFiles(new Configuration(), outputDir.toString(), x ->
                pattern.matcher(x.getName()).matches());
        assertEquals(NUMBER_OF_OUTPUT_FILES, fileCount);

        List<Text> out = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputDir.toString()));
        List<String> actualValues = out.stream().map(Text::toString).sorted().collect(Collectors.toList());
        List<String> expectedValues = in.collect().stream().map(x -> x._2.toString()).sorted().collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualValues, expectedValues);
    }
}
