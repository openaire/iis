package eu.dnetlib.iis.common.javamapreduce.hack;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.cli.ParseException;

import eu.dnetlib.iis.common.utils.AvroUtils;

/**
 * Avro schema generator. Takes schema class names as inputs and provides
 * schema content under the keys being schema class names.
 * @author mhorst
 *
 */
public final class AvroSchemaGenerator {

    //------------------------ CONSTRUCTORS --------------------------
    
    private AvroSchemaGenerator() {}
    
    //------------------------ LOGIC ---------------------------------
    
    /**
     * Generate Avro schema JSON string from a class name
     * 
     * @param className Fully qualified class name of the Avro record
     * @return Schema JSON string ready for Spark's option("avroSchema", schema)
     * @throws ClassNotFoundException if the class cannot be found
     */
    public static String getSchemaString(String className) throws ClassNotFoundException {
        return getSchema(className).toString();
    }
    
    /**
     * Provides Avro schema defined for a given class name being {@link SpecificRecordBase} subclass.
     * 
     * @param className Fully qualified class name of the Avro record
     * @return schema class defined for {@link SpecificRecordBase}
     * @throws ClassNotFoundException if the class cannot be found
     */
    public static Schema getSchema(String className) throws ClassNotFoundException {
        Class<?> recordClass = Class.forName(className);
        if (SpecificRecordBase.class.isAssignableFrom(recordClass)) {
            return SpecificData.get().getSchema(recordClass);
        } else {
            throw new IllegalArgumentException("Provided class is not a SpecificRecord: " + className);
        }
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        if (args.length==0) {
        	throw new RuntimeException("no classes provided for schema generation");
        }
        File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
        Properties props = new Properties();
        OutputStream os = new FileOutputStream(file);
        for (int i=0; i<args.length; i++) {
        	if (args[i]!=null && args[i].length()>0) {
        		props.setProperty(args[i], AvroUtils.toSchema(args[i]).toString());	
        	}
        }
        try {
        	props.store(os, "");
        } finally {
        	os.close();	
        }
    }
}
