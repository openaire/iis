package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.avro.Country;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class AvroSaverTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    private static File workingDir;

    private static String outputDirPath;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory(AvroSaverTest.class.getSimpleName() + "_").toFile();
        outputDirPath = workingDir + "/spark_sql_avro_cloner/output";
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }

    //------------------------ TESTS --------------------------

    @Test
    public void test() throws IOException {
        // given
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark Avro Saver Test")
                .addJobProperty("spark.driver.host", "localhost")
                .setMainClass(AvroSaverTest.class)
                .build();

        // execute
        executor.execute(sparkJob);

        // assert
        List<Country> countries = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(4, countries.size());
        assertEquals(1, countries.stream().filter(c -> c.getIso().equals("PL")).count());
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.host", "localhost");

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(conf)) {
            Dataset<Row> countries = spark.read()
                    .json("src/test/resources/eu/dnetlib/iis/common/avro/countries.json");

            // without these 2 lines below there is no guarantee as to the field order and then
            // they can be saved not in accordance with avro schema
            countries.registerTempTable("countries");
            countries = spark
                    .sql("select id, name, iso from countries");

            AvroSaver.save(countries, Country.SCHEMA$, outputDirPath);
        }
    }
}
