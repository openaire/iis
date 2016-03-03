package eu.dnetlib.iis.common.hive;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/*
 * The code was taken from http://stackoverflow.com/questions/6445339/collect-set-in-hive-keep-duplicates
 */
public class CollectAllUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis)
            throws SemanticException {
        if (tis.length != 1) {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly one argument is expected.");
        }
        return new CollectAllEvaluator();
    }

    public static class CollectAllEvaluator extends GenericUDAFEvaluator {

        private ObjectInspector inputOI;
        private StandardListObjectInspector loi;
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputOI = parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {

            ArrayList<Object> container;
        }

        @Override
        public void reset(AggregationBuffer ab)
                throws HiveException {
            ((ArrayAggregationBuffer) ab).container = new ArrayList<Object>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer()
                throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
                agg.container.add(ObjectInspectorUtils.copyToStandardObject(p, this.inputOI));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab)
                throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            ArrayList<Object> ret = new ArrayList<Object>(agg.container.size());
            ret.addAll(agg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer ab, Object o)
                throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            ArrayList<Object> partial = (ArrayList<Object>) internalMergeOI.getList(o);
            for (Object i : partial) {
                agg.container.add(ObjectInspectorUtils.copyToStandardObject(i, this.inputOI));
            }
        }

        @Override
        public Object terminate(AggregationBuffer ab)
                throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) ab;
            ArrayList<Object> ret = new ArrayList<Object>(agg.container.size());
            ret.addAll(agg.container);
            return ret;
        }
    }
}