package eu.dnetlib.iis.wf.importer.wiley;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;


/**
 * Creates a {@link DocumentText} avro data from an input {@link DocumentText} datastore by updating both id and text fields.
 * Id field is set with a DOI record available in the text payload field holding WileyML record.
 * Text field is set to an appropriate text representation of the WileyML record. 
 * 
 * @author mhorst
 * 
 */
public class ImportWileyXmlContentJob {

    public static void main(String[] args) {
        JobParams params = new JobParams();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        runWithSparkSession(new SparkConf(), params.isSparkSessionShared, spark -> {

            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputPath);

            AvroDataFrameReader avroDataFrameReader = new AvroDataFrameReader(spark);

            Dataset<Row> inputDocumentTextDF = avroDataFrameReader.read(params.inputPath, DocumentText.SCHEMA$);
            
            String xPathQueryDOI = params.xPathQueryDOI;
            
            Dataset<Row> idAndTextDF = inputDocumentTextDF
                    .select(get_xml_object_string(xPathQueryDOI).apply(col("text")).as("id"), col("text"))
                    .where(col("text").isNotNull().and(col("text").notEqual("").and(col("id").isNotNull().and(col("id").notEqual("")))));

            Dataset<Row> documentTextDF = spark.createDataFrame(idAndTextDF.javaRDD(),
                    (StructType) SchemaConverters.toSqlType(DocumentText.SCHEMA$).dataType());

            new AvroDataFrameWriter(documentTextDF).write(params.outputPath, DocumentText.SCHEMA$);
        });
    }

    public static UserDefinedFunction get_xml_object_string(String xPathStr) {
        return udf((UDF1<String, String>) xml -> {

            XPath xPath = XPathFactory.newInstance().newXPath();
            
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document xmlDocument = builder.parse(new InputSource(new StringReader(xml)));
            
            return extractFirstNonEmptyTrimmedTextContent((NodeList) xPath.compile(xPathStr).evaluate(xmlDocument, XPathConstants.NODESET));

        }, DataTypes.StringType);
    }
    
    private static String extractFirstNonEmptyTrimmedTextContent(NodeList nodes) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node currentNode = nodes.item(i);
            String textContent = currentNode.getTextContent();
            if (StringUtils.isNotBlank(textContent)) {
                return textContent.trim();
            }
        }
        return null;
    }

    @Parameters(separators = "=")
    public static class JobParams {

        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;

        @Parameter(names = "-inputPath", required = true)
        private String inputPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-xPathQueryDOI", required = true)
        private String xPathQueryDOI;
    }
}