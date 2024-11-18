package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.io.ByteArrayOutputStream;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;
import org.jdom.Element;
import org.xml.sax.SAXException;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.common.importer.CermineAffiliationBuilder;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;
import pl.edu.icm.cermine.exception.AnalysisException;
import pl.edu.icm.cermine.exception.TransformationException;
import pl.edu.icm.cermine.metadata.affiliation.CRFAffiliationParser;

/**
 * 
 * Simple command line tool accepting affiliation string, to be provided as a single cmdline argument 
 * (so needs to be specified in quotes), and producing extracted affiliations metadata.
 * 
 * @author mhorst
 */
public class AffiliationMetadataExtractorMain {

    private static final String encoding = "utf-8";
    
    private static final CermineAffiliationBuilder cermineAffiliationBuilder = new CermineAffiliationBuilder();
    private static final CermineToIngestAffConverter cermineToIngestAffConverter = new CermineToIngestAffConverter();
    
    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            Affiliation aff = buildAffiliationFromText(args[0]);
            if (aff == null) {
                throw new RuntimeException("unable to build affiliation from string: " + args[0]);
            }
            DatumWriter<Affiliation> writer = new SpecificDatumWriter<>(Affiliation.class);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(Affiliation.getClassSchema(), stream);
            writer.write(aff, jsonEncoder);
            jsonEncoder.flush();
            System.out.println(new String(stream.toByteArray(), encoding));
        } else {
            throw new RuntimeException("invalid number of input arguments: " + args.length 
                    + ", expecting a single argument with an affiliation string!");
        }
    }
    
    /**
     * Parses string representation of an affiliation and extracts affiliation metadata.
     * @param affiliationText affiliation text to be parsed
     * @throws SAXException thrown whenever affiliation parsing fails
     */
    private static Affiliation buildAffiliationFromText(String affiliationText) throws SAXException {
        try {
            if (StringUtils.isNotBlank(affiliationText)) {
                CRFAffiliationParser affiliationParser = new CRFAffiliationParser();
                Element parsedAffiliation = affiliationParser.parse(affiliationText);
                if (parsedAffiliation!=null) {
                    CermineAffiliation cAff = cermineAffiliationBuilder.build(parsedAffiliation);
                    return cermineToIngestAffConverter.convert(cAff);
                }
            }
        } catch (TransformationException | AnalysisException e) {
            throw new SAXException("unexpected exception while parsing "
                    + "affiliations for document", e);
        }
        
        return null;
    }
}
