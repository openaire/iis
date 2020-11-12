package eu.dnetlib.iis.common.spark;

import eu.dnetlib.iis.common.SlowTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Objects;

/**
 * Support for tests using {@link org.apache.spark.SparkContext} - extends this class to access SparkContext in local mode.
 */
@SlowTest
public class TestWithSharedSparkContext {
    private static transient SparkContext _sc;
    private static transient JavaSparkContext _jsc;
    protected boolean initialized = false;

    protected SparkContext sc() {
        return _sc;
    }

    protected JavaSparkContext jsc() {
        return _jsc;
    }

    @BeforeEach
    public void beforeEach() {
        initialized = Objects.nonNull(_sc);

        if (!initialized) {
            SparkConf conf = new SparkConf();
            conf.setAppName(getClass().getSimpleName());
            conf.setMaster("local");
            conf.set("spark.driver.host", "localhost");
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            _sc = new SparkContext(conf);
            _jsc = new JavaSparkContext(_sc);
        }
    }

    @AfterAll
    public static void afterAll() {
        _sc.stop();
        _sc = null;
        _jsc = null;
    }
}
