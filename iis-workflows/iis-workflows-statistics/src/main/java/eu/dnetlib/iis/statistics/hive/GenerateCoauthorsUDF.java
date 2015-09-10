package eu.dnetlib.iis.statistics.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Dominika Tkaczyk
 */
public class GenerateCoauthorsUDF extends GenericUDF {

    private StringObjectInspector authorIdOI;
    private MapObjectInspector mapOI;
    private StringObjectInspector mapKeyOI;
    private IntObjectInspector mapValueOI;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("GenerateCoauthorsUDF takes 2 argument: string, map<string, int>");
        }
       
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The argument must be a list");
        }
        
        authorIdOI = (StringObjectInspector) arguments[0];
        
        if (!(arguments[1] instanceof MapObjectInspector)) {
            throw new UDFArgumentException("The argument must be a list");
        }
        
        mapOI = (MapObjectInspector) arguments[1];
        
        if (!(mapOI.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The argument must be a list");
        }
        
        mapKeyOI = (StringObjectInspector) mapOI.getMapKeyObjectInspector();
        
        if (!(mapOI.getMapValueObjectInspector() instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The argument must be a list");
        }
        
        mapValueOI = (IntObjectInspector) mapOI.getMapValueObjectInspector();
        
        List names = Arrays.asList("id", "coauthoredPapersCount");
        List ois = Arrays.asList(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        
        return ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getStandardStructObjectInspector(names, ois));
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List coauthors = new ArrayList();
        String key = authorIdOI.getPrimitiveJavaObject(arguments[0].get());
        Map map = mapOI.getMap(arguments[1].get());
        if (!map.isEmpty()) {
            for (Object entry : map.entrySet()) {
                Map.Entry mapEntry = (Map.Entry<Object, Object>) entry;
                String id = mapKeyOI.getPrimitiveJavaObject(mapEntry.getKey());
                if (!id.equals(key)) {
                    int count = mapValueOI.get(mapEntry.getValue());
                    Object[] coauthor = new Object[2];
                    coauthor[0] = new Text(id);
                    coauthor[1] = new IntWritable(count);
                    coauthors.add(coauthor);
                }
            }
        }
             
        return coauthors;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "GenerateCoauthors()";
    }
    
}
