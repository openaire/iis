package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Function;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.Job;
import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SparkSessionSupportTest {

    @Nested
    class RunWithSparkSessionTest {

        @Test
        @DisplayName("SparkSession is not stopped")
        public void givenSparkSessionBuilderAndJob_whenRunWithSharedSparkSession_thenSparkSessionIsNotStopped() throws Exception {
            SparkSession spark = mock(SparkSession.class);
            SparkConf conf = mock(SparkConf.class);
            Function<SparkConf, SparkSession> sparkSessionBuilder = mock(Function.class);
            when(sparkSessionBuilder.apply(conf)).thenReturn(spark);
            Job job = mock(Job.class);

            runWithSparkSession(sparkSessionBuilder, conf, true, job);

            verify(sparkSessionBuilder).apply(conf);
            verify(job).accept(spark);
            verify(spark, never()).stop();
        }

        @Test
        @DisplayName("SparkSession is stopped")
        public void givenSparkSessionBuilderAndJob_whenRunWithNotSharedSparkSession_thenSparkSessionIsStopped() throws Exception {
            SparkSession spark = mock(SparkSession.class);
            SparkConf conf = mock(SparkConf.class);
            Function<SparkConf, SparkSession> sparkSessionBuilder = mock(Function.class);
            when(sparkSessionBuilder.apply(conf)).thenReturn(spark);
            Job job = mock(Job.class);

            runWithSparkSession(sparkSessionBuilder, conf, false, job);

            verify(sparkSessionBuilder).apply(conf);
            verify(job).accept(spark);
            verify(spark, times(1)).stop();
        }
    }
}