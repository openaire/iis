/*
 * Copyright (c) 2014-2014 ICM UW
 */

package eu.dnetlib.iis.testutils;

import org.apache.pig.data.DataType;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.parameters.ParseException;

import java.io.IOException;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 *         Created: 28.05.2014 15:47
 */
public class PigJsonTest extends PigTest {
    public PigJsonTest(String scriptPath) throws IOException {
        super(scriptPath);
    }

    public PigJsonTest(String[] script) {
        super(script);
    }

    public PigJsonTest(String scriptPath, String[] args) throws IOException {
        super(scriptPath, args);
    }

    public PigJsonTest(String[] script, String[] args) {
        super(script, args);
    }

    public PigJsonTest(String[] script, String[] args, String[] argsFile) {
        super(script, args, argsFile);
    }

    public PigJsonTest(String scriptPath, String[] args, String[] argFiles) throws IOException {
        super(scriptPath, args, argFiles);
    }

    public PigJsonTest(String scriptPath, String[] args, PigServer pig, Cluster cluster) throws IOException {
        super(scriptPath, args, pig, cluster);
    }

    public void loadFromJson(String alias, String path, String schema) throws IOException, ParseException {
        // TODO debug why it doesn't work
        // registerScript();
        // StringBuilder sb = new StringBuilder();
        // Schema.stringifySchema(sb, getPigServer().dumpSchema(alias), DataType.TUPLE);

        override(alias, String.format(
                "%s = LOAD '%s' using JsonLoader('%s');",
                alias,
                path,
                schema
                // TODO debug why it doesn't work
                // sb.toString()
        ));
    }

    // TODO public void assertJsonOutput(String alias, String path);
    // for instance: store to temp file using
    //   store filtered_cleaned into 'output.json' using JsonStorage();
    // then read it and compare with expected output


}
