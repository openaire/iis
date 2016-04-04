package eu.dnetlib.iis.wf.top.json2avro;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;

import com.googlecode.protobuf.format.JsonFormat;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 *
 */
public class SimpleJsonVerifier {

    public static void main(String[] args) throws Exception {
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        String resourcePath = "C:/workspace/iis/iis-wf/iis-wf-top/src/test/resources/eu/dnetlib/iis/wf/top/integration/import/input/meta/organization/1/organization#body";
        String oafText = IOUtils.toString(new FileInputStream(new File(resourcePath)));
        try {
            JsonFormat.merge(oafText, oafBuilder);  
        } catch (Exception e) {
            throw new RuntimeException("got exception when parsing text resource: " +
                    resourcePath + ", text content: " + oafText, e);
        }
    }

}
