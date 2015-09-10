package eu.dnetlib.iis.common.model.extrainfo.converter;

import java.io.StringWriter;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.extended.NamedMapConverter;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;

/**
 * Abstract xml converter.
 * @author mhorst
 *
 */
public abstract class AbstractExtraInfoConverter<T> implements ExtraInfoConverter<T> {

	protected final XStream xstream;
	
	public AbstractExtraInfoConverter() {
		xstream = new XStream(new DomDriver());
		xstream.setMode(XStream.NO_REFERENCES);
		
//		removing class attribute because no unmarshalling is required
		xstream.aliasSystemAttribute(null, "class");
//		changing the way maps are generated
		NamedMapConverter namedMapConverter = new NamedMapConverter(
				xstream.getMapper(),"entry","key",String.class,"value",Integer.class,
				true, true, xstream.getConverterLookup());
		xstream.registerConverter(namedMapConverter);
	
	}
	
	@Override
	public String serialize(T object) {
		StringWriter sw = new StringWriter();
//		xstream.marshal(object, new CompactWriter(sw));
		xstream.marshal(object, new PrettyPrintWriter(sw));
		return sw.toString();
	}
	
	@Override
	public T deserialize(String source) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("deserialization is unsupported");
	}

}
