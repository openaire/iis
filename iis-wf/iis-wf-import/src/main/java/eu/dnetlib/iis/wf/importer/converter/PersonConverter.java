package eu.dnetlib.iis.wf.importer.converter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.iis.importer.schemas.Person;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * HBase {@link Result} to avro {@link Person} converter.
 * @author mhorst
 *
 */
public class PersonConverter extends AbstractAvroConverter<Person> {

	
	protected static final Logger log = Logger.getLogger(PersonConverter.class);


	/**
	 * @param encoding
	 * @param resultApprover
	 */
	public PersonConverter(String encoding,
			ResultApprover resultApprover) {
		super(encoding, resultApprover);
	}
	
	/**
	 * Builds objects.
	 * @param source
	 * @param resolvedOafObject
	 * @param encoding
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	@Override
	public Person buildObject(Result source, Oaf resolvedOafObject) throws UnsupportedEncodingException {
		eu.dnetlib.data.proto.PersonProtos.Person sourcePerson = resolvedOafObject.getEntity()!=null?
				resolvedOafObject.getEntity().getPerson():null;
		if (sourcePerson==null) {
			log.error("skipping: no person object " +
					"for a row " + new String(source.getRow(), getEncoding()));
			return null;
		}
		if (resolvedOafObject.getEntity().getId()!=null) {
			if (sourcePerson.getMetadata()!=null) {
				Person.Builder builder = Person.newBuilder();
				builder.setId(resolvedOafObject.getEntity().getId());
				if (sourcePerson.getMetadata().getFirstname()!=null &&
						sourcePerson.getMetadata().getFirstname().getValue()!=null &&
						!sourcePerson.getMetadata().getFirstname().getValue().isEmpty()) {
					builder.setFirstname(sourcePerson.getMetadata().getFirstname().getValue());
				}
				if (sourcePerson.getMetadata().getSecondnamesList()!=null &&
						!sourcePerson.getMetadata().getSecondnamesList().isEmpty()) {
					if (builder.getSecondnames()==null) {
						builder.setSecondnames(
								new ArrayList<CharSequence>(sourcePerson.getMetadata().getSecondnamesList().size()));
					}
					List<CharSequence> secondNames = new ArrayList<CharSequence>(
							sourcePerson.getMetadata().getSecondnamesList().size());
					for (StringField currentSecondName : sourcePerson.getMetadata().getSecondnamesList()) {
						if (currentSecondName.getValue()!=null) {
							secondNames.add(currentSecondName.getValue());
						}
					}
					builder.getSecondnames().addAll(secondNames);
				}
				if (sourcePerson.getMetadata().getFullname()!=null && 
						sourcePerson.getMetadata().getFullname().getValue()!=null &&
						!sourcePerson.getMetadata().getFullname().getValue().isEmpty()) {
					builder.setFullname(sourcePerson.getMetadata().getFullname().getValue());
				}
				return builder.build();
			} else {
				log.error("skipping: no metadata specified for " +
						"person of a row " + new String(source.getRow(), getEncoding()));
				return null;
			}
		} else {
			log.error("skipping: no id specified for " +
					"person of a row " + new String(source.getRow(), getEncoding()));
			return null;
		}
	}
	
}
