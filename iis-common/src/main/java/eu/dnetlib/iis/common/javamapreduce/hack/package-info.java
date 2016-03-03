/**
 * Provides classes necessary to omit specifying explicitly Avro JSON schemas
 * in Oozie XML workflow definition. These schemas have to be specified
 * for data ingested by mapper, passed between mapper and reducer, and produced
 * by reducer.
 * <p/>
 * Unfortunately this is a very hackish way of realizing this goal, since we're
 * injecting to Oozie a modified version of some pseudo-internal classes used
 * by the Avro-based map-reduce. In future versions of the Avro library, the
 * functioning of these pseudo-internal classes might change, which might
 * render this hack inadequate.
 * <p/>
 * Implementing our own data workflow description language will make this
 * hack unnecessary since the JSON schemas that normally have to be present
 * in the Oozie file would be generated automatically along with the rest of
 * Oozie XML file.
 */
package eu.dnetlib.iis.common.javamapreduce.hack;