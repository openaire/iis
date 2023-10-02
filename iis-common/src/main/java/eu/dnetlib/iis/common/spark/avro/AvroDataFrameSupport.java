package eu.dnetlib.iis.common.spark.avro;

import java.io.Serializable;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Support for dataframes of avro types.
 * 
 * @author mhorst
 */
public class AvroDataFrameSupport implements Serializable {
    
    private static final long serialVersionUID = -3980871922050483460L;

    private AvroDataFrameSupport() {
    }

    /**
     * @param <T> type of elements
     * @param dataFrame seq with elements for the dataframe
     * @param clazz class of objects in the dataset
     * @return Dataset of objects corresponding to records in the given dataframe
     */
    public static <T extends SpecificRecordBase> Dataset<T> toDS(final Dataset<Row> dataFrame, final Class<T> clazz) {
        final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        return (Dataset<T>) dataFrame.toJSON().map((MapFunction<String, T>) json -> (T) mapper.readValue(json, clazz),
                Encoders.kryo((Class<T>) clazz));
    }
}
