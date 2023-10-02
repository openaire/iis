package eu.dnetlib.iis.common.spark.avro;

import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Support for writing dataframes of avro types.
 * 
 * @author mhorst 
 */
public class AvroDataFrameWriter implements Serializable {
    
    private static final long serialVersionUID = 7842491849433906246L;
    
    private final Dataset<Row> dataFrame;

    /**
     * Default constructor accepting DataFrame. 
     * 
     * @param dataFrame DataFrame of avro type
     */
    public AvroDataFrameWriter(Dataset<Row> dataFrame) {
        this.dataFrame = dataFrame;
    }

    /**
     * Writes a dataframe as avro datastore using avro schema.
     * @param path path to the data store
     * @param avroSchema Avro schema of the records
     */
    public void write(String path, Schema avroSchema) {
        dataFrame.write().format("avro").option("avroSchema", avroSchema.toString())
                .option("compression", "uncompressed").save(path);
    }
}
