package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * {@link Oaf} field decoder.
 * 
 * @author mhorst
 *
 */
public class OafFieldDecoder implements FieldDecoder {

	/**
	 * Decodes {@link Oaf} object from byte[].
	 * 
	 * @param source byte array
	 * @return {@link Oaf} object
	 * @throws FieldDecoderException
	 */
	@Override
	public Oaf decode(Object source) throws FieldDecoderException {
		try {
			OafProtos.Oaf.Builder builder = OafProtos.Oaf.newBuilder();
			builder.mergeFrom((byte[]) source);
			return builder.build();
		} catch (InvalidProtocolBufferException e) {
			throw new FieldDecoderException("unable to decode protocol buffer object", e);
		}
	}

	@Override
	public boolean canHandle(Object source) {
		return (source != null && source.getClass().equals(byte[].class));
	}
}
