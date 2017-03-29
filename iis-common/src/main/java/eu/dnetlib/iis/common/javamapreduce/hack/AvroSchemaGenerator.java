package eu.dnetlib.iis.common.javamapreduce.hack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

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
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        if (args.length==0) {
        	throw new RuntimeException("no classes provided for schema generation");
        }
        File file = new File(System.getProperty("oozie.action.output.properties"));
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
