package eu.dnetlib.iis.wf.export.actionmanager.entity;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * {@link AtomicAction} class serialization and deserialization utilities.
 * @author mhorst
 *
 */
public class AtomicActionSerDeUtils {

    /**
     * Returns deserialized {@link AtomicAction} payload.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Oaf> T getPayload(String serializedAction) throws IOException, ClassNotFoundException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        JsonNode rootNode = objectMapper.readTree(serializedAction);
        
        return (T) objectMapper.readValue(objectMapper.treeAsTokens(rootNode.get("payload")), (Class<T>) Class.forName(rootNode.get("clazz").textValue()));
    }
    
    /**
     * Returns deserialized {@link AtomicAction} payload.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Oaf> AtomicAction<T> deserializeAction(String serializedAction) throws IOException, ClassNotFoundException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        JsonNode rootNode = objectMapper.readTree(serializedAction);
        Class<T> clazz = (Class<T>) Class.forName(rootNode.get("clazz").textValue());

        AtomicAction<T> action = new AtomicAction<T>();
        action.setClazz(clazz);
        action.setPayload((T) objectMapper.readValue(objectMapper.treeAsTokens(rootNode.get("payload")), clazz));
        return action;
    }
    
}
