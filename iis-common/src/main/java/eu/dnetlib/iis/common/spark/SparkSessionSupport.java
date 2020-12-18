package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;
import java.util.function.Function;

/**
 * SparkSession utility methods.
 */
public class SparkSessionSupport {
    private SparkSessionSupport() {
    }

    @FunctionalInterface
    public interface Job {
        void accept(SparkSession spark) throws Exception;
    }

    /**
     * Runs a given job using SparkSession created using default builder and supplied SparkConf. Stops SparkSession
     * when SparkSession is shared e.g. created in tests. Allows to reuse SparkSession created externally.
     *
     * @param conf               SparkConf instance
     * @param sharedSparkSession When true will not stop SparkSession
     * @param job                Job using constructed SparkSession
     */
    public static void runWithSparkSession(SparkConf conf,
                                           Boolean sharedSparkSession,
                                           Job job) {
        runWithSparkSession(
                SparkSessionFactory::withConfAndKryo,
                conf,
                sharedSparkSession,
                job);
    }

    /**
     * Runs a given job using SparkSession created using supplied builder and supplied SparkConf. Stops SparkSession
     * when SparkSession is shared e.g. created in tests. Allows to reuse SparkSession created externally.
     *
     * @param conf               SparkConf instance
     * @param sharedSparkSession When true will not stop SparkSession
     * @param job                Job using constructed SparkSession
     */
    public static void runWithSparkSession(Function<SparkConf, SparkSession> sparkSessionBuilder,
                                           SparkConf conf,
                                           Boolean sharedSparkSession,
                                           Job job) {
        runWithSparkSession(
                sparkSessionBuilder.apply(conf),
                sharedSparkSession,
                job);
    }

    private static void runWithSparkSession(SparkSession spark,
                                            Boolean sharedSparkSession,
                                            Job job) {
        try {
            job.accept(spark);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (Objects.nonNull(spark) && !sharedSparkSession) {
                spark.stop();
            }
        }
    }
}
