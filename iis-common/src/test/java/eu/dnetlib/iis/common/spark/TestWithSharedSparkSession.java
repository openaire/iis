package eu.dnetlib.iis.common.spark;

import eu.dnetlib.iis.common.SlowTest;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Objects;

/**
 * Support for tests using {@link SparkSession} - extends this class to access SparkSession in local mode.
 */
@SlowTest
public class TestWithSharedSparkSession {
    private static SparkSession _spark;
    protected boolean initialized = false;

    public SparkSession spark() {
        return _spark;
    }

    @BeforeEach
    public void beforeEach() {
        initialized = Objects.nonNull(_spark);

        if (!initialized) {
            SparkConf conf = new SparkConf();
            conf.setAppName(getClass().getSimpleName());
            conf.setMaster("local");
            conf.set("spark.driver.host", "localhost");
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            _spark = SparkSession.builder().config(conf).getOrCreate();
        }
    }

    @AfterAll
    public static void afterAll() {
        _spark.stop();
        _spark = null;
    }
}
