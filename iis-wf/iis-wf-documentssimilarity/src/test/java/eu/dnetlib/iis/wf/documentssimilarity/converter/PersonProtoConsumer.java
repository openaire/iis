package eu.dnetlib.iis.wf.documentssimilarity.converter;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.documentssimilarity.protobuf.PersonProtos.Person;

/**
 * Person consumer expecting single person record at input with the field values defined as parameters.
 * 
 * @author mhorst
 *
 */
public class PersonProtoConsumer {

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        AvroToRdbTransformerJobParameters params = new AvroToRdbTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        final int id = params.personId;
        final String name = params.personName;
        final int age = params.personAge;
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {

            JavaPairRDD<BytesWritable, BytesWritable> rdd = sc.sequenceFile(params.input, BytesWritable.class, BytesWritable.class);
            JavaRDD<Person> protoRDD = rdd.values().map(bytesWritable -> {
            	Person.Builder builder = Person.newBuilder();
                try {
                    builder.mergeFrom(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                    return builder.build();
                } catch (Exception e) {
                    throw new IOException("unable to read protocol buffer object from bytes", e);
                }
            
            });

            Preconditions.checkArgument(protoRDD.count() == 1, "unexpected number of person records: %d, expected 1", protoRDD.count());
            checkPerson(protoRDD.first(), id, name, age);
        }
    }
    
    //------------------------ PRIVATE --------------------------

    private static void checkPerson(Person person, int id, String name, int age) throws IOException{
    	Preconditions.checkArgument(id == person.getId(), 
    			String.format("person id does not match! expected: %d, got: %d", id, person.getId()));
    	Preconditions.checkArgument(name.equals(person.getName()), 
    			String.format("person name does not match! expected: %s, got: %s", name, person.getName()));
    	Preconditions.checkArgument(age == person.getAge(), 
    			String.format("person age does not match! expected: %d, got: %d", age, person.getAge()));
    }
    
    @Parameters(separators = "=")
    private static class AvroToRdbTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-personId", required = true)
        private int personId;
        
        @Parameter(names = "-personName", required = true)
        private String personName;
        
        @Parameter(names = "-personAge", required = true)
        private int personAge;
    }

}