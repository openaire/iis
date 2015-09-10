package eu.dnetlib.iis.core.hive;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 *
 * @author Dominika Tkaczyk
 */
public class CreateEmptyArrayUDF extends GenericUDF {
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("CreateEmptyArray takes only 1 argument");
        }
       
        return ObjectInspectorFactory.getStandardListObjectInspector(arguments[0]);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return new ArrayList<Object>();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "CreateEmptyArray()";
    }
    
}
