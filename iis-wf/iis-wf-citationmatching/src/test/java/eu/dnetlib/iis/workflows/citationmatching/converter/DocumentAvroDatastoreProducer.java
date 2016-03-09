package eu.dnetlib.iis.workflows.citationmatching.converter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/** Produce data stores
 * @author Mateusz Fedoryszak
 */
public class DocumentAvroDatastoreProducer implements Process {

    private final static String documentPort = "document";

    public Map<String, PortType> getInputPorts() {
        return new HashMap<String, PortType>();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return createOutputPorts();
    }

    private static Map<String, PortType> createOutputPorts(){
        HashMap<String, PortType> outputPorts =
                new HashMap<String, PortType>();
        outputPorts.put(documentPort,
                new AvroPortType(DocumentMetadata.SCHEMA$));
        return outputPorts;
    }

    public static List<DocumentMetadata> getDocumentMetadataList() {
        BasicMetadata basic1 = new BasicMetadata(
                Lists.<CharSequence>newArrayList("Jan Kowalski"),
                "A new method of something", "Journal of something", "1-2", "2001");
        BasicMetadata basic2 = new BasicMetadata(
                Lists.<CharSequence>newArrayList("Jan Kowalski", "Zygmunt Nowak"),
                "How to do it?", "Something Journal", "2-3", "2002");
        BasicMetadata basic3 = new BasicMetadata(
                Lists.<CharSequence>newArrayList("Zygmunt Nowak"),
                "Our great tool", "Everything Journal", "3-4", "2003");
        BasicMetadata basic4 = new BasicMetadata(
                Lists.<CharSequence>newArrayList("Zygmunt Nowak", "Jan Kowalski"),
                "Big and great system", "Advances in everything", "4-5", "2004");

        DocumentMetadata doc1 = new DocumentMetadata(
                "1", basic1,
                Lists.newArrayList(new ReferenceMetadata(1, basic2, null), new ReferenceMetadata(2, basic3, null)));
        DocumentMetadata doc2 = new DocumentMetadata(
                "2", basic2,
                Lists.newArrayList(new ReferenceMetadata(1, basic3, null), new ReferenceMetadata(2, basic4, null)));

        return Lists.newArrayList(doc1, doc2);
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf,
                    Map<String, String> parameters)	throws IOException{
        Map<String, Path> output = portBindings.getOutput();
        FileSystem fs = FileSystem.get(conf);

        DataStore.create(getDocumentMetadataList(),
                new FileSystemPath(fs, output.get(documentPort)));
    }

}