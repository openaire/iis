package eu.dnetlib.iis.common.spark;

import eu.dnetlib.iis.common.SlowTest;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import org.apache.spark.sql.SparkSession.Builder;

import java.util.Objects;

/**
 * Support for tests using {@link SparkSession} - extends this class to access SparkSession in local mode.
 */
@SlowTest
public class TestWithSharedSparkSession {
    private static SparkSession _spark;
    protected boolean initialized = false;
    protected final boolean hiveSupportEnabled;

    /**
     * Default constructor with hive support in spark session disabled.
     */
    public TestWithSharedSparkSession() {
        this(false);
    }
    
    /**
     * @param hiveSupportEnabled indicates whether hive support should be enabled in spark session
     */
    public TestWithSharedSparkSession(boolean hiveSupportEnabled) {
        this.hiveSupportEnabled = hiveSupportEnabled;
    }
    
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
            conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
            Builder builder = SparkSession.builder().config(conf);
            if (hiveSupportEnabled) {
                builder = builder.enableHiveSupport();
            }
            _spark = builder.getOrCreate();
        }
    }

    @AfterAll
    public static void afterAll() {
        _spark.stop();
        _spark = null;
    }
}
