package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterIOUtils.clearOutput;
import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterIOUtils.readCitations;
import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterIOUtils.storeReportEntries;
import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterIOUtils.storeSerializedActions;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

@ExtendWith(MockitoExtension.class)
class CitationRelationExporterIOUtilsTest extends TestWithSharedSparkSession {

    @Test
    @DisplayName("Output locations are removed")
    public void givenMockRemoveFunction_whenOutputIsCleared_thenMockIsUsed() {
        Consumer<String> removeFn = mock(Consumer.class);

        clearOutput("path/to/relations", "path/to/report", removeFn);

        verify(removeFn, atLeastOnce()).accept("path/to/relations");
        verify(removeFn, atLeastOnce()).accept("path/to/report");
    }

    @Test
    @DisplayName("Citations are read from input")
    public void givenMockReadFunction_whenCitationsAreRead_thenMockIsUsed() {
        Function<String, Dataset<Row>> readFn = mock(Function.class);
        Dataset<Row> citationsDF = mock(Dataset.class);
        when(readFn.apply("path/to/citations")).thenReturn(citationsDF);

        Dataset<Row> result = readCitations("path/to/citations", readFn);

        assertThat(result, sameInstance(citationsDF));
    }

    @Test
    @DisplayName("Serialized actions are stored in output")
    public void givenMockWriteFunction_whenSerializedActionsAreStored_thenMockIsUsed() {
        BiConsumer<JavaPairRDD<Text, Text>, String> writeFn = mock(BiConsumer.class);
        Dataset<Text> serializedActions = spark().createDataset(Collections.singletonList(
                new Text("content")
        ), Encoders.kryo(Text.class));

        storeSerializedActions(serializedActions, "path/to/relations", writeFn);

        ArgumentCaptor<JavaPairRDD<Text, Text>> javaPairRDDCaptor = ArgumentCaptor.forClass(JavaPairRDD.class);
        verify(writeFn, atLeastOnce()).accept(javaPairRDDCaptor.capture(), eq("path/to/relations"));
        assertEquals(Collections.singletonList("content"),
                javaPairRDDCaptor.getValue().map(Tuple2::_2).map(Text::toString).collect());
    }

    @Test
    @DisplayName("Report entries are stored in output")
    public void givenMockWriteFunction_whenReportEntriesAreStored_thenMockIsUsed() {
        SparkAvroSaver avroSaver = mock(SparkAvroSaver.class);
        BiConsumer<Dataset<ReportEntry>, String> writeFn = mock(BiConsumer.class);
        Dataset<ReportEntry> repartitionedReportEntries = mock(Dataset.class);
        Dataset<ReportEntry> reportEntries = mock(Dataset.class);
        when(reportEntries.repartition(1)).thenReturn(repartitionedReportEntries);

        storeReportEntries(avroSaver, reportEntries, "path/to/report", writeFn);

        verify(writeFn, atLeastOnce()).accept(repartitionedReportEntries, "path/to/report");
    }
}