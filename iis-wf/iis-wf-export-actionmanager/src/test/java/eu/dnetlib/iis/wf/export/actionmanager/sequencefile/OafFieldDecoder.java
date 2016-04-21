package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.io.UnsupportedEncodingException;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * {@link Oaf} field decoder.
 * @author mhorst
 *
 */
public class OafFieldDecoder implements FieldDecoder {

	/**
	 * Decodes {@link Oaf} object from byte[].
	 * @param source byte array
	 * @return {@link Oaf} object
	 * @throws InvalidProtocolBufferException
	 * @throws UnsupportedEncodingException
	 */
	@Override
	public Oaf decode(Object source) throws Exception {
		OafProtos.Oaf.Builder builder = OafProtos.Oaf.newBuilder();
		builder.mergeFrom((byte[]) source);
		return builder.build();
	}

	@Override
	public boolean canHandle(Object source) {
		return (source!=null && source.getClass().isArray() 
				&& source.getClass().getComponentType().isPrimitive())
				&& byte.class.isAssignableFrom(source.getClass().getComponentType());
	}
}
