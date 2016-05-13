package eu.dnetlib.iis.wf.importer;

import java.util.Arrays;

import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.JsonFormat.ParseException;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * {@link Oaf} helper class.
 * @author mhorst
 *
 */
public class OafHelper {
	
	/**
	 * Builds {@link Oaf} object from byte array.
	 * @param input byte array input
	 * @return {@link Oaf} object built from byte array.
	 * @throws InvalidProtocolBufferException
	 */
	public static Oaf buildOaf(byte[] input) throws InvalidProtocolBufferException {
		Oaf.Builder oafBuilder = Oaf.newBuilder();
		oafBuilder.mergeFrom(input);
		return oafBuilder.build();
	}
	
	/**
     * Builds {@link Oaf} object from JSON representation.
     * @param input JSON representation of {@link Oaf} object
	 * @throws ParseException 
     */
    public static Oaf buildOaf(String input) throws ParseException {
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        JsonFormat.merge(input, oafBuilder);
        return oafBuilder.build();
    }
	
	/**
	  * Copies array or returns null when source is null.
	 * @param source
	 * @return copied array
	 */
	final public static byte[] copyArrayWhenNotNull(byte[] source) {
		if (source!=null) {
			return Arrays.copyOf(source, source.length);
		} else {
			return null;
		}
	}
}
