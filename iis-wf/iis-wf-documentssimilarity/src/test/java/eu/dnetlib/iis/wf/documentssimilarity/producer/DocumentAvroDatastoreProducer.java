package eu.dnetlib.iis.wf.documentssimilarity.producer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import de.svenjacobs.loremipsum.LoremIpsum;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.Person;

/**
 * Produce data stores
 *
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

    private static Map<String, PortType> createOutputPorts() {
        HashMap<String, PortType> outputPorts =
                new HashMap<String, PortType>();
        outputPorts.put(documentPort,
                new AvroPortType(DocumentMetadata.SCHEMA$));
        return outputPorts;
    }
    
    public static Person createPerson(String id, String name) {
        return new Person(id, null, null, name);
    }

    public static List<DocumentMetadata> getDocumentMetadataList() {
        DocumentMetadata doc1 = new DocumentMetadata();
        doc1.setId("1");
        doc1.setTitle("A new method of something");
        doc1.setAbstract$("We present a new method of doing something. We are not sure yet what " +
                        "it is actually doing, but it definitely is a worthwhile technique.");
        doc1.setKeywords(Lists.<CharSequence>newArrayList("method", "something", "nothing", "anything"));
        doc1.setAuthors(Lists.<Person>newArrayList(createPerson("1", "Jan Kowalski")));

        DocumentMetadata doc2 = new DocumentMetadata();
        doc2.setId("2");
        doc2.setTitle("How to do it?");
        doc2.setAbstract$("We are asking some of fundamental engineering questions here. As all " +
                "kinds of fundamental questions, they probably have no answers.");
        doc2.setKeywords(Lists.<CharSequence>newArrayList(
                "doing things", "questioning", "falsificationism", "epistemology"));
        doc2.setAuthors(Lists.<Person>newArrayList(createPerson("1", "Jan Kowalski"), createPerson("2", "Zygmunt Nowak")));

        DocumentMetadata doc3 = new DocumentMetadata();
        doc3.setId("3");
        doc3.setTitle("Our great tool");
        doc3.setAbstract$("In this paper we present Our great tool that is capable of doing " +
                "anything. First theoretical studies have shown its great potential. Practical " +
                "applications are to be investigated in the future.");
        doc3.setKeywords(Lists.<CharSequence>newArrayList(
                "Our great tool", "perpetuum mobile", "stop problem", "P==NP?"));
        doc3.setAuthors(Lists.<Person>newArrayList(createPerson("2", "Zygmunt Nowak")));

        DocumentMetadata doc4 = new DocumentMetadata();
        doc4.setId("4");
        doc4.setTitle("Big and great system");
        doc4.setAbstract$("Worldwide amount of data is growing every year. That is why ever " +
                "bigger and greater systems needs to be built. In this paper we present our biggest " +
                "and greatest system so far.");
        doc4.setKeywords(Lists.<CharSequence>newArrayList(
                "big", "enormous", "great", "grand"));
        doc4.setAuthors(Lists.<Person>newArrayList(createPerson("2", "Zygmunt Nowak"), createPerson("1", "Jan Kowalski")));

        List<DocumentMetadata> results = Lists.newArrayList(doc1, doc2, doc3, doc4);
        
//      adding dummy records up to 10 in total
        LoremIpsum loremIpsum = new LoremIpsum();
        Random rand = new Random();
        for(int i = 5; i<=200; i++) {
        	DocumentMetadata doc = new DocumentMetadata();
            doc.setId(Integer.toString(i));
            doc.setTitle(loremIpsum.getWords(10, rand.nextInt(50)));
            doc.setAbstract$(loremIpsum.getWords(50, rand.nextInt(50)));
            results.add(doc);
        }

        return results;
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf,
                    Map<String, String> parameters) throws IOException {
        Map<String, Path> output = portBindings.getOutput();
        FileSystem fs = FileSystem.get(conf);

        DataStore.create(getDocumentMetadataList(),
                new FileSystemPath(fs, output.get(documentPort)));
    }
    
}