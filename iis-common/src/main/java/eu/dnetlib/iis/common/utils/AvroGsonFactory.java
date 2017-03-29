package eu.dnetlib.iis.common.utils;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * Factory for gson object that supports serializing avro generated classes
 * 
 * @author madryk
 *
 */
public final class AvroGsonFactory {
    
    //------------------------ CONSTRUCTORS -------------------
    
    
    private AvroGsonFactory() {}

	
	//------------------------ LOGIC --------------------------
	
	public static Gson create() {
		GsonBuilder builder = new GsonBuilder();

		builder.registerTypeAdapter(CharSequence.class, new CharSequenceDeserializer());

		return builder.create();
	}

	public static class CharSequenceDeserializer implements JsonDeserializer<CharSequence> {

		@Override
		public CharSequence deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			return json.getAsString();
		}

	}
}
