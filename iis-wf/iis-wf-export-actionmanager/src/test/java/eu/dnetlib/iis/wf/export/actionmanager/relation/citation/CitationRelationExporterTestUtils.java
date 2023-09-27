package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.export.schemas.Citations;

/**
 * Shared utility methods used by several unit tests.
 * 
 * @author mhorst
 */
public class CitationRelationExporterTestUtils {

    protected static Citations createCitations(String documentId, List<CitationEntry> citationEntries) {
        return Citations.newBuilder()
                .setDocumentId(documentId)
                .setCitations(new GenericData.Array<>(Citations.SCHEMA$.getField("citations").schema(), citationEntries))
                .build();
    }
    
    protected static Dataset<Row> createDataFrame(SparkSession sparkSession, List<Citations> inputList) {
        List<Row> dataFrameList = new ArrayList<>(inputList.size());
        for (Citations input : inputList) {
            List<Row> citationsList = new ArrayList<>(input.getCitations().size());
            for (CitationEntry citation : input.getCitations()) {
                citationsList.add(RowFactory.create(citation.getPosition(), citation.getRawText(),
                        citation.getDestinationDocumentId(), citation.getConfidenceLevel(),
                        // JavaConverters.mapAsScalaMapConverter(citation.getExternalDestinationDocumentIds()).asScala()));
                        // new HashMap<String, String>()));
                        citation.getExternalDestinationDocumentIds()));
            }
            dataFrameList.add(RowFactory.create(input.getDocumentId(), 
                    citationsList.toArray()));
        }
        return sparkSession.createDataFrame(dataFrameList, CitationRelationExporterTestUtils.getCitationsSchema());
    }
    
    protected static StructType getCitationsSchema() {
        // FIXME trying with explicitly defined schema to avoid Avro schema inclusion (hoping to fix: NotSerializableException: org.apache.avro.Schema$RecordSchema)
        // it turns out the exception is still raised which is quite surprising
        // and this way of defining schema makes CitationRelationExporterUtilsTest failing
//        return StructType$.MODULE$.apply(
//                Arrays.asList(
//                      StructField$.MODULE$.apply("documentId", DataTypes.StringType, false, Metadata.empty()),
//                      StructField$.MODULE$.apply("citations", DataTypes.createArrayType(DataTypes.createStructType(
//                               Arrays.asList(
//                                       StructField$.MODULE$.apply("position", DataTypes.IntegerType, true, Metadata.empty()),
//                                       StructField$.MODULE$.apply("rawText", DataTypes.StringType, false, Metadata.empty()),
//                                       StructField$.MODULE$.apply("destinationDocumentId", DataTypes.StringType, true, Metadata.empty()),
//                                       StructField$.MODULE$.apply("confidenceLevel", DataTypes.FloatType, false, Metadata.empty()),
//                                       StructField$.MODULE$.apply("externalDestinationDocumentIds", DataTypes.createMapType(
//                                               DataTypes.StringType, DataTypes.StringType), false, Metadata.empty())
//                                       ))), true, Metadata.empty())
//              )
//    );
        return (StructType) SchemaConverters.toSqlType(Citations.SCHEMA$).dataType();
    }
    
}
