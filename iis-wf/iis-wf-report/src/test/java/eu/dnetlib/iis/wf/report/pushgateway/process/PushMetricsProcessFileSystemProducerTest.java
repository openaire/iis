package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class PushMetricsProcessFileSystemProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.FileSystemProducer fileSystemProducer = new PushMetricsProcess.FileSystemProducer();

        // when
        Optional<FileSystem> result = fileSystemProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.FileSystemProducer fileSystemProducer = new PushMetricsProcess.FileSystemProducer();

        // when
        Optional<FileSystem> result = fileSystemProducer.create(new Configuration());

        // then
        assertTrue(result.isPresent());
    }

}
