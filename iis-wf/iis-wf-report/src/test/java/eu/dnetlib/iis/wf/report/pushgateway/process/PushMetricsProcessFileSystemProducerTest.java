package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
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
