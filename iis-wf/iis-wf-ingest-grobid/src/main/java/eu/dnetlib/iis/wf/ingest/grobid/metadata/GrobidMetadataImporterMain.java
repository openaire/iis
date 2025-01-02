package eu.dnetlib.iis.wf.ingest.grobid.metadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.grobid.client.GrobidClient;
import org.grobid.core.data.BiblioItem;
import org.grobid.core.engines.Engine;
import org.grobid.core.factory.GrobidFactory;
import org.grobid.core.main.GrobidHomeFinder;
import org.grobid.core.utilities.GrobidProperties;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;

/**
 * This class is responsible for performing metadata extraction from PDF files using Grobid.
 */
public class GrobidMetadataImporterMain {
    
    private static final String encoding = "utf-8";
    private static final String parsedFileSuffix = ".parsed.json";
    
    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            if (StringUtils.isBlank(args[0])) {
                throw new RuntimeException("No valid location provided!");
            }
            
            File file = new File(args[0]);
            if (file.isDirectory()) {
                String[] fileLocs = file.list((dir, name) -> !name.endsWith(parsedFileSuffix));
                for (String fileLoc : fileLocs) {
                    processFile(new File(file, fileLoc).getAbsolutePath());
                }
            } else {
                processFile(args[0]);
            }
        } else {
            throw new RuntimeException("invalid number of input arguments: " + args.length 
                    + ", expecting a single argument with PDF file location!");
        }
    }
    
    private static void processFile(String fileLocation) throws Exception {
        // Initialize Grobid
        GrobidHomeFinder grobidHomeFinder = new GrobidHomeFinder(List.of("grobid-home"));
        GrobidProperties.getInstance(grobidHomeFinder);
        
        Engine engine = GrobidFactory.getInstance().createEngine();
        
        // Process PDF
        BiblioItem result = engine.processHeader(fileLocation, false);
        
        // Map to our schema
        ExtractedDocumentMetadata.Builder builder = ExtractedDocumentMetadata.newBuilder();
        builder.setId("");
        builder.setText("");
        
        if (result.getTitle() != null) {
            builder.setTitle(result.getTitle());
        }
        
        if (result.getAbstract() != null) {
            builder.setAbstract(result.getAbstract());
        }
        
        if (result.getAuthors() != null) {
            List<Author> authors = result.getAuthors().stream()
                .map(a -> Author.newBuilder()
                    .setAuthorFullName(a.getFullName())
                    .build())
                .toList();
            builder.setAuthors(authors);
        }
        
        if (result.getAffiliations() != null) {
            List<Affiliation> affiliations = result.getAffiliations().stream()
                .map(a -> Affiliation.newBuilder()
                    .setOrganization(a.getInstitution())
                    .build())
                .toList();
            builder.setAffiliations(affiliations);
        }
        
        if (result.getJournal() != null) {
            builder.setJournal(result.getJournal());
        }
        
        if (result.getYear() != null) {
            builder.setYear(Integer.parseInt(result.getYear()));
        }
        
        // Serialize to JSON
        DatumWriter<ExtractedDocumentMetadata> writer = new SpecificDatumWriter<>(ExtractedDocumentMetadata.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(ExtractedDocumentMetadata.getClassSchema(), stream);
        writer.write(builder.build(), jsonEncoder);
        jsonEncoder.flush();
        
        String fileContent = new String(stream.toByteArray(), encoding);
        System.out.println(fileContent);
        FileUtils.writeStringToFile(new File(fileLocation + parsedFileSuffix), fileContent, encoding);
    }
}
