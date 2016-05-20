package eu.dnetlib.iis.wf.importer.infospace;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * InfoSpace record holding columnFamily, qualifier and JSON representation of Oaf object.
 * @author mhorst
 *
 */
public class InfoSpaceRecord implements Writable {

    private Text columnFamily;

    private Text qualifier;
    
    private Text oafJson;
    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public InfoSpaceRecord() {
        this.columnFamily = new Text();
        this.qualifier = new Text();
        this.oafJson = new Text();
    }
    
    public InfoSpaceRecord(Text columnFamily, Text qualifier, Text oafJson) {
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        this.oafJson = oafJson;
    }

    // ------------------------ GETTERS --------------------------
    
    public Text getColumnFamily() {
        return columnFamily;
    }

    public Text getQualifier() {
        return qualifier;
    }

    public Text getOafJson() {
        return oafJson;
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public void readFields(DataInput in) throws IOException {
        columnFamily.readFields(in);
        qualifier.readFields(in);
        oafJson.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        columnFamily.write(out);
        qualifier.write(out);
        oafJson.write(out);
    }

}
