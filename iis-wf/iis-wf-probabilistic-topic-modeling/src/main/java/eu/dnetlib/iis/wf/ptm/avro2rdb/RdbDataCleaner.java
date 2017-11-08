package eu.dnetlib.iis.wf.ptm.avro2rdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Purges data from the set of relational database tables provided as configuration parameter.
 * 
 * @author mhorst
 *
 */
public class RdbDataCleaner implements Process {


    private static final String PARAM_TABLE_NAMES_CSV = "tableNamesCSV";
    
    private static final String PARAM_RDB_URL = "rdbUrl";
    
    private static final String PARAM_RDB_USERNAME = "rdbUsername";
    
    private static final String PARAM_RDB_PASSWORD = "rdbPassword";
    
    private static final String VALID_TABLE_NAME_REGEX = "^[a-zA-Z_][a-zA-Z0-9_]*$";

    
    private static final Logger log = Logger.getLogger(RdbDataCleaner.class);
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        String tableNamesCSV = parameters.get(PARAM_TABLE_NAMES_CSV);
        String rdbUrl = parameters.get(PARAM_RDB_URL);
        String rdbUsername = parameters.get(PARAM_RDB_USERNAME);
        String rdbPassword = parameters.get(PARAM_RDB_PASSWORD);
        
        Preconditions.checkNotNull(tableNamesCSV, "table names are unspecified, input parameter missing: " + PARAM_TABLE_NAMES_CSV);
        Preconditions.checkNotNull(rdbUrl, "rdb url is unspecified, input parameter missing: " + PARAM_RDB_URL);
        Preconditions.checkNotNull(rdbUsername, "rdb user name is unspecified, input parameter missing: " + PARAM_RDB_USERNAME);
        Preconditions.checkNotNull(rdbPassword, "rdb user password is unspecified, input parameter missing: " + PARAM_RDB_PASSWORD);
        
        String[] tableNames = Arrays.asList(StringUtils.split(tableNamesCSV, ',')).stream().map(x -> x.trim()).toArray(String[]::new);
        
        for (String tableName : tableNames) {
            if (!isTableNameValid(tableName)) {
                throw new Exception("invalid table name: " + tableName);
            }
        }
        
        try (Connection con = connect(rdbUrl, rdbUsername, rdbPassword)) {
            for (String tableName : tableNames) {
                int count = con.prepareStatement("DELETE FROM " + tableName).executeUpdate();
                log.info("number of records deleted from table '" + tableName + "': " + count);
            }
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }
    
    // -------------------------------- PRIVATE ----------------------------------------

    private static boolean isTableNameValid(String tableName) throws Exception {
        return tableName.matches(VALID_TABLE_NAME_REGEX);
    }
    
    private Connection connect(String rdbUrl, String rdbUsername, String rdbPassword) throws SQLException {
        return DriverManager.getConnection(rdbUrl, rdbUsername, rdbPassword);
    }
    
}
