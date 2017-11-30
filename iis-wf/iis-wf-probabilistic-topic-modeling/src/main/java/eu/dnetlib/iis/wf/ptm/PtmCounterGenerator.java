package eu.dnetlib.iis.wf.ptm;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Counter generator process. Obtains counts from relational database.
 * 
 * @author mhorst
 *
 */
public class PtmCounterGenerator implements Process {

    
    protected static final String COUNTER_TOPIC = "TOPIC_COUNTER";
    
    protected static final String COUNTER_PUB_TOPIC = "PUBTOPIC_COUNTER";
    
    protected static final String PARAM_RDB_URL = "rdbUrl";
    
    protected static final String PARAM_RDB_USERNAME = "rdbUsername";
    
    protected static final String PARAM_RDB_PASSWORD = "rdbPassword";
    
    protected static final String PARAM_EXPERIMENT_ID = "experimentId";
    
    private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();

    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        String experimentId = parameters.get(PARAM_EXPERIMENT_ID);
        String rdbUrl = parameters.get(PARAM_RDB_URL);
        String rdbUsername = parameters.get(PARAM_RDB_USERNAME);
        String rdbPassword = parameters.get(PARAM_RDB_PASSWORD);
        
        Preconditions.checkNotNull(experimentId, "experiment identifier is unspecified, input parameter missing: " + PARAM_EXPERIMENT_ID);
        Preconditions.checkNotNull(rdbUrl, "rdb url is unspecified, input parameter missing: " + PARAM_RDB_URL);
        Preconditions.checkNotNull(rdbUsername, "rdb user name is unspecified, input parameter missing: " + PARAM_RDB_USERNAME);
        Preconditions.checkNotNull(rdbPassword, "rdb user password is unspecified, input parameter missing: " + PARAM_RDB_PASSWORD);
        
        NamedCounters counters = new NamedCounters(new String[] { COUNTER_TOPIC, COUNTER_PUB_TOPIC });
        
        try (Connection con = connect(rdbUrl, rdbUsername, rdbPassword)) {
            
            PreparedStatement statement = con.prepareStatement("SELECT COUNT(*) FROM topicdescription where experimentId = ?");
            statement.setString(1, experimentId);
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                counters.increment(COUNTER_TOPIC, rs.getLong(1));
            }
            
            statement = con.prepareStatement("SELECT COUNT(*) FROM pubtopic where experimentId = ?");
            statement.setString(1, experimentId);
            rs = statement.executeQuery();
            while(rs.next()) {
                counters.increment(COUNTER_PUB_TOPIC, rs.getLong(1));
            }
            
        }
        
        countersWriter.writeCounters(counters, System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

    protected Connection connect(String rdbUrl, String rdbUsername, String rdbPassword) throws SQLException {
        return DriverManager.getConnection(rdbUrl, rdbUsername, rdbPassword);
    }
    
}
