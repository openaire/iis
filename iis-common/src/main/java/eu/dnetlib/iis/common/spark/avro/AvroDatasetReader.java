package eu.dnetlib.iis.common.spark.avro;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Support for reading avro datastores as datasets.
 * 
 * @author mhorst
 */
public class AvroDatasetReader implements Serializable {
    
    private static final long serialVersionUID = 4858427693578954728L;
    
    private final SparkSession sparkSession;

    /**
     * Default constructor accepting spark session as parameter.
     * @param sparkSession spark session
     */
    public AvroDatasetReader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * Reads avro datastore as Spark dataset using avro schema and kryo encoder.
     *
     * NOTE: due to inability to use bean-based encoder for avro types this method uses kryo encoder;
     * for this reason this method creates objects by mapping rows to jsons and jsons to instances of objects.
     * 
     * @param <T> type of objects in the dataset
     * @param path path to the data store
     * @param avroSchema Avro schema of the records
     * @param clazz class of objects in the dataset
     * @return Dataset with data read from given path
     */
    public <T extends SpecificRecordBase> Dataset<T> read(String path, Schema avroSchema, Class<T> clazz) {
        return AvroDataFrameSupport.toDS(new AvroDataFrameReader(sparkSession).read(path, avroSchema), clazz);
    }
}
