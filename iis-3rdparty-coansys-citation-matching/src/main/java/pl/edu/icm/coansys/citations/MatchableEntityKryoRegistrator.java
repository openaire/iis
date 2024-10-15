package pl.edu.icm.coansys.citations;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Kryo registrator that is able to serialize {@link MatchableEntity} objects.
 * 
 * @author madryk
 */
public class MatchableEntityKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(MatchableEntity.class, new MatchableEntitySerializer());
    }


    public static class MatchableEntitySerializer extends Serializer<MatchableEntity> {

        @Override
        public void write(Kryo kryo, Output output, MatchableEntity object) {
            byte[] bytes = object.data().toByteArray();
            
            output.writeInt(bytes.length, true);
            output.writeBytes(bytes);
        }

        @Override
        public MatchableEntity read(Kryo kryo, Input input, Class<MatchableEntity> type) {
            int length = input.readInt(true);
            byte[] bytes = input.readBytes(length);
            
            return MatchableEntity.fromBytes(bytes);
        }

    }
    
}
