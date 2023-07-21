package eu.dnetlib.iis.wf.documentssimilarity.converter;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.protobuf.AvroToProtoBufConverter;
import eu.dnetlib.iis.documentssimilarity.protobuf.PersonProtos;

/**
 * Simple converter class to be used for testing avro to protocol buffer converter workflow.
 * @author mhorst
 *
 */
public class PersonAvroToProtoBufConverter implements AvroToProtoBufConverter<Person, PersonProtos.Person> {
    @Override
    public String convertIntoKey(Person datum) {
        return datum.getId().toString();
    }

    @Override
    public PersonProtos.Person convertIntoValue(Person datum) {
    	PersonProtos.Person.Builder builder = PersonProtos.Person.newBuilder();
    	builder.setId(datum.getId());
    	builder.setName(datum.getName());
    	builder.setAge(datum.getAge());
        return builder.build();
    }

}
