package eu.dnetlib.iis.wf.export.actionmanager;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import scala.Tuple2;

/**
 * Utility class for serializing actions into text representation.
 * 
 * @author mhorst
 *
 */
public class ActionSerializationUtils {

    /**
     * Maps action RDD into text RDD.
     */
    public static <T extends Oaf> JavaPairRDD<Text, Text> mapActionToText(JavaRDD<AtomicAction<T>> actions) {
        ObjectMapper objectMapper = new ObjectMapper();
        return actions.mapToPair(action -> new Tuple2<>(new Text(""), new Text(serializeAction(action, objectMapper))));
    }
    
    /**
     * Serializes action into text representation.
     */
    public static <T extends Oaf> String serializeAction(AtomicAction<T> action, ObjectMapper objectMapper) throws JsonProcessingException {
        return objectMapper.writeValueAsString(action);
    }
    
}
