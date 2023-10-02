package eu.dnetlib.iis.common.spark.avro;

import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

/**
 * Support for reading avro datastores as dataframes.
 * 
 * @author mhorst
 * 
 */
public class AvroDataFrameReader implements Serializable {
   
    private static final long serialVersionUID = 4858427693578954728L;
    
    private final SparkSession sparkSession;

    /**
     * Default constructor accepting spark session as parameter.
     * @param sparkSession spark session
     */
    public AvroDataFrameReader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * @param path Path to the data store
     * @param avroSchema Avro schema of the records
     * @return DataFrame with data read from given path
     */
    public Dataset<Row> read(String path, Schema avroSchema) {
        Dataset<Row> in = sparkSession.read().format("avro").option("avroSchema", avroSchema.toString()).load(path);
        return sparkSession.createDataFrame(in.rdd(), (StructType) SchemaConverters.toSqlType(avroSchema).dataType());
    }
}
