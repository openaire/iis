package eu.dnetlib.iis.workflows.transformers.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 */
public class IdReplacerUDF extends EvalFunc<Tuple> {

    /**
     * From the result we remove two first fields of the input tuple. This is because two first fields are UDF
     * parameters, the rest is the input record in which we replace ids.
     */
    private static final int FIELDS_TO_REMOVE = 2;

    @Override
    /**
     * UDF replaces original id with new id string. The input tuple should contain following elements:
     * a dot-separated list of field ids representing a path in the input tree structure (first element),
     * new id string (second element), the rest of the tuple should contain the consecutive fields of the input record.
     *
     * Replacing of multiple id fields is not implemented yet. Only one field replacing is implemented.
     *
     * @param tuple a tuple (id path - dot-separated list of fields indicating position in the record schema tree, new id string, input tuple fields...)
     * @return
     * @throws IOException
     */
    public Tuple exec(Tuple input) throws IOException {
        checkArguments(input);

        String oldIdPath = (String) input.get(0);
        String newId = (String) input.get(1);
        Tuple record = getRecord(input);
        Schema recordSchema = getRecordSchema(getInputSchema());

        replaceIdFieldInTuple(newId, oldIdPath, record, recordSchema);

        return record;
    }

    private static void checkArguments(Tuple input) throws IllegalArgumentException {
        checkNotNull(input);
        checkInputSize(input);
    }

    private static void checkInputSize(Tuple input) throws IllegalArgumentException {
        final int minimalInputSize = FIELDS_TO_REMOVE + 1;
        final int actualSize = input.size();
        if (actualSize < minimalInputSize)
            throw new IllegalArgumentException(
                    "Not enough arguments passed to " + IdReplacerUDF.class.getName() +
                            ". Expected at least " + minimalInputSize +
                            ", but got " + actualSize + " arguments.");
    }

    private static void checkNotNull(Tuple input) throws IllegalArgumentException {
        if (input == null)
            throw new IllegalArgumentException(
                    IdReplacerUDF.class.getName() + ": Input tuple cannot be null");
    }

    /**
     * Leaves only fields belonging to the input record, removes fields corresponding to other UDF parameters.
     */
    private static Tuple getRecord(Tuple input) throws ExecException {/*-?|2014-04-23 IdReplacer with 2 IDs to replace|mafju|c2|?*/
        List<Object> strippedInput = new ArrayList<Object>();
        for (int i = FIELDS_TO_REMOVE; i < input.size(); i++) {
            strippedInput.add(input.get(i));
        }

        return TupleFactory.getInstance().newTuple(strippedInput);
    }

    /**
     * Leaves only fields belonging to the input record, removes fields corresponding to other UDF parameters.
     */
    private static Schema getRecordSchema(Schema inputSchema) throws FrontendException {
        Schema output = new Schema();
        for (int i = FIELDS_TO_REMOVE; i < inputSchema.size(); i++) {
            output.add(inputSchema.getField(i));
        }
        return output;
    }

    private static void replaceIdFieldInTuple(String newId, String oldIdPath, Tuple tuple, Schema tupleSchema) throws ExecException, FrontendException {
        if (newId != null) {
            Pair<Tuple, Integer> idTupleAndPos = getIdField(oldIdPath, tuple, tupleSchema);
            idTupleAndPos.getLeft().set(idTupleAndPos.getRight(), newId);
        }
    }

    /**
     * Extracts from an input record the tuple containing the last field in a path and position of that field in that tuple.
     * Traverses the record schema tree using path. Path is a list of field names indicating position in that tree.
     * <p/>
     * In other words: returns a (sort-of-a) lens allowing modification of a field indicated by idPath.
     * More on lenses: http://stackoverflow.com/a/5597750/257401
     *
     * @param path   dot-separated list of field names
     * @param record input record
     * @param schema input record schema
     * @return a pair (the tuple containing the last field in a path, position of that field in that tuple)
     * @throws FrontendException
     * @throws ExecException
     */
    private static Pair<Tuple, Integer> getIdField(String path, Tuple record, Schema schema) throws FrontendException, ExecException {
        String[] fields = path.split("\\.");
        for (int i = 0; i < fields.length - 1; i++) {
            int pos = schema.getPosition(fields[i]);
            schema = schema.getField(fields[i]).schema;
            record = (Tuple) record.get(pos);
        }

        return new ImmutablePair<Tuple, Integer>(record, schema.getPosition(fields[fields.length - 1]));
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
        	return new Schema(new Schema.FieldSchema(getSchemaName(
            		getClass().getName().toLowerCase(), 
            		input),
            		getRecordSchema(input), DataType.TUPLE));
        } catch (FrontendException ex) {
            return null;
        }
    }

}
