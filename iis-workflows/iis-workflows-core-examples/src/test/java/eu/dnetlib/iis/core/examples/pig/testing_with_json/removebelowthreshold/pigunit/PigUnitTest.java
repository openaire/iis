/*
 * Copyright (c) 2014-2014 ICM UW
 */

package eu.dnetlib.iis.core.examples.pig.testing_with_json.removebelowthreshold.pigunit;

import eu.dnetlib.iis.core.common.AvroUtils;
import eu.dnetlib.iis.testutils.PigJsonTest;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 *         Created: 28.05.2014 12:44
 */
public class PigUnitTest {
	
    @Test
    public void testJsonInput() throws IOException, ParseException {
        // Parameters to be substituted in Pig Latin script before the
        // test is run.  Format is one string for each parameter,
        // parameter=value
        final String[] params = {
                // Actual parameters
                "threshold=1.5",
                "schema_input_from_project_reference_extraction="+ AvroUtils.toSchema("eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject"),
                "schema_output="+ AvroUtils.toSchema("eu.dnetlib.iis.importer.schemas.DocumentToProject"),

                // Dummy parameters.
                // Load statements in pig script will be replaced by PigUnit
                // input* parameters are useless, but they need to be
                // passed to avoid "Undefined parameter" error.
                "input_from_project_reference_extraction=x",
                "output=x",
        };
        PigJsonTest test = new PigJsonTest("src/main/resources/eu/dnetlib/iis/core/examples/pig/testing_with_json/removebelowthreshold/oozie_app/lib/scripts/removebelowthreshold.pig", params);

        // Run the example script using the input from json file
        // rather than loading whatever the load statement says.
        // "input_from_project_reference_extraction" is the alias to override with the input data.
        test.loadFromJson(
                "input_from_project_reference_extraction",
                "src/test/resources/eu/dnetlib/iis/core/examples/pig/testing_with_json/removebelowthreshold/data/input_from_project_reference_extraction.json",
                "documentId:chararray,projectId:chararray,confidenceLevel:double");

        final String[] output = {
                "(id-2,248095)",
                "(id-8,240763)",
                "(id-8,275840)",
        };

        // "filtered_cleaned" is the alias
        // to test against the value(s) in output.
        test.assertOutput("filtered_cleaned", output);
    }

//    @Test
    public void testTextInput() throws IOException, ParseException {
        // Parameters to be substituted in Pig Latin script before the
        // test is run.  Format is one string for each parameter,
        // parameter=value
        final String[] params = {
                // Actual parameters
                "threshold=1.5",
                "schema_input_from_project_reference_extraction="+ AvroUtils.toSchema("eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject"),
                "schema_output="+ AvroUtils.toSchema("eu.dnetlib.iis.importer.schemas.DocumentToProject"),

                // Dummy parameters.
                // Load statements in pig script will be replaced by PigUnit
                // input* parameters are useless, but they need to be
                // passed to avoid "Undefined parameter" error.
                "input_from_project_reference_extraction=x",
                "output=x",
        };
        PigTest test = new PigTest("src/main/resources/eu/dnetlib/iis/core/examples/pig/testing_with_json/removebelowthreshold/oozie_app/lib/scripts/removebelowthreshold.pig", params);

        // Rather than read from a file, generate synthetic input.
        // Format is one record per line, tab-separated.
        final String[] input = {
                "id-2\t248095\t1.5766148184367308",
                "id-3\t300820\t0.8652002245558127",
                "id-3\t275103\t1.3005493344846906",
                "id-8\t240763\t1.6733200530681511",
                "id-8\t275840\t1.6053482043291596",
        };

        final String[] output = {
                "(id-2,248095)",
                "(id-8,240763)",
                "(id-8,275840)",
        };

        // Run the example script using the input we constructed
        // rather than loading whatever the load statement says.
        // "input_from_project_reference_extraction" is the alias to override with the input data.
        // "filtered_cleaned" is the alias
        // to test against the value(s) in output.
        test.assertOutput("input_from_project_reference_extraction", input, "filtered_cleaned", output);
    }
}
