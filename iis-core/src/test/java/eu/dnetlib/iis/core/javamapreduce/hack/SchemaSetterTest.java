package eu.dnetlib.iis.core.javamapreduce.hack;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import eu.dnetlib.iis.core.schemas.standardexamples.Document;
import eu.dnetlib.iis.core.schemas.standardexamples.Person;
import eu.dnetlib.iis.core.schemas.standardexamples.personwithdocuments.PersonWithDocuments;
import eu.dnetlib.iis.core.javamapreduce.hack.oldapi.SchemaSetter;

public class SchemaSetterTest {

	@Test
	public void testBasic() {
		Configuration conf = new Configuration();
		conf.set(SchemaSetter.inputClassName, 
			"eu.dnetlib.iis.core.schemas.standardexamples.Document");
		conf.set(SchemaSetter.mapOutputKeyClassName,
			SchemaSetter.primitiveTypePrefix+"DOUBLE");
		conf.set(SchemaSetter.mapOutputValueClassName,
			"eu.dnetlib.iis.core.schemas.standardexamples.Person");
		conf.set(SchemaSetter.outputClassName,
			"eu.dnetlib.iis.core.schemas.standardexamples.personwithdocuments.PersonWithDocuments");
		SchemaSetter.set(conf);
		Assert.assertEquals(Document.SCHEMA$.toString(), 
				conf.get(SchemaSetter.avroInput));
		Assert.assertEquals(Pair.getPairSchema(
					Schema.create(Schema.Type.DOUBLE),
					Person.SCHEMA$).toString(), 
				conf.get(SchemaSetter.avroMapOutput));
		Assert.assertEquals(PersonWithDocuments.SCHEMA$.toString(),
				conf.get(SchemaSetter.avroOutput));
				
	}

}
