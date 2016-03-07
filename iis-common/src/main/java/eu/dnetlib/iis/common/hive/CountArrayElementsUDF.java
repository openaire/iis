package eu.dnetlib.iis.common.hive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *
 * @author Dominika Tkaczyk
 */
public class CountArrayElementsUDF extends GenericUDF {

    private ListObjectInspector listOI;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("CountArrayElements takes only 1 argument: array<T>");
        }
       
        if (!(arguments[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("The argument must be a list");
        }
        
        listOI = (ListObjectInspector) arguments[0];
        
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                listOI.getListElementObjectInspector(), 
                PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        
        List list = (List) listOI.getList(arguments[0].get());
        if (list == null) {
            return null;
        }
        
        Map<Object, Integer> map = new HashMap<Object, Integer>();
        for (Object object: list) {
            if (map.get(object) == null) {
                map.put(object, 0);
            }
            map.put(object, map.get(object)+1);
        }
    
        return map;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "CountArrayElements()";
    }
    
}
