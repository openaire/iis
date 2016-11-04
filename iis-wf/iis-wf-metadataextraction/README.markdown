#Metadata extraction

IIS module responsible for metadata and fulltext extraction. Based on ICM [CERMINE library](https://github.com/CeON/CERMINE).

Contains all required classes and testing workflow.

##I/O
port type | avro shema
------------ | -------------
input | [`eu.dnetlib.iis.importer.schemas.DocumentContent`](https://github.com/openaire/iis/blob/cdh5/iis-schemas/src/main/avro/eu/dnetlib/iis/importer/DocumentContent.avdl)
output | [`eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata`](https://github.com/openaire/iis/blob/cdh5/iis-schemas/src/main/avro/eu/dnetlib/iis/metadataextraction/ExtractedDocumentMetadata.avdl)
output | [`eu.dnetlib.iis.audit.schemas.Fault`](https://github.com/openaire/iis/blob/cdh5/iis-schemas/src/main/avro/eu/dnetlib/iis/audit/Fault.avdl)

All the mappings between IIS metadata output model and Cermine NLM are described on google docs site:

https://docs.google.com/spreadsheets/d/1dECy6oYr-LY8Zcudl_KU-uAxUxygiF3kCxJ6MSQhLmk/edit#gid=2

in `cermine_to_iis` tab.

`Fault` records contain all the errors occurred while processing `DocumentContent` input records.

##Metadata extraction sanity test

In order to be sure `iis-wf-metadataextraction` module handles PDF files properly without fatal failures such as:

* `OutOfMemoryError`
* hangups

causing whole IIS processing interruption set of sanity tests can be prepared.

`eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMain` serves this purpose by extracting both text and metadata from PDF files provided as commandline arguments. Both explicit file names and directory names can be provided as input. When providing directory name all the `*.pdf` files from this directory and subdirectories will be processed.

This way metadata extraction sanity test can be triggered (e.g. by CI such as jenkins) from commandline by executing:

```
mvn test-compile exec:java -Dexec.mainClass="eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorMain" -Dexec.args="path/to/pdf/files" -Dexec.classpathScope=test
```
inside `iis-wf/iis-wf-metadataextraction` module directory.

###Defining expectations

By default sanity test suite succeeds when no exception is thrown while parsing full set of PDF files. If one needs to define more finegrained expectations related to processing of particular document then set of expectation properties can be defined. This can be achieved by creating `.expectations` file next to the pdf document. Same filename with different extension have to be used.

The following properties are supported:

expectation property key | description
------------ | -------------
`timeout.secs` | processing timeout expressed in seconds (overriding default 10 minutes)
`exception.class` | expected exception class name
`exception.message` | expected exception message
`metadata.[field.path]` | [`ExtractedDocumentMetadata`](https://github.com/openaire/iis/blob/cdh5/iis-schemas/src/main/avro/eu/dnetlib/iis/metadataextraction/ExtractedDocumentMetadata.avdl) field expected value

####Exception expectations

Knowing given document should cause particular exception to be thrown one can define `*.expectations` file with the following content:
```
exception.class=pl.edu.icm.cermine.exception.AnalysisException
exception.message=Invalid PDF file
```
causing sanity test to succeed when such exception is thrown.

####Metadata field expectations

If document is expected to be parsed properly without any errors one can define set of field expectations:
```
metadata.title=This is main title
metadata.references[0].text=[1] Expected reference text
metadata.references[0].position=1
metadata.references[0].basicMetadata.year=2016
metadata.affiliations[0].organization=Interdisciplinary Centre for Mathematical and Computational Modelling
```
where property key is field path to be resolved by [`org.apache.commons.beanutils.PropertyUtils`](https://commons.apache.org/proper/commons-beanutils/apidocs/org/apache/commons/beanutils/PropertyUtils.html#getNestedProperty-java.lang.Object-java.lang.String-) (part of `commons-beanutils` project). If provided value can be matched by [`ValueSpecMatcher`](https://github.com/openaire/iis/blob/cdh5/iis-common/src/test/java/eu/dnetlib/iis/common/report/test/ValueSpecMatcher.java) with given field value from [`ExtractedDocumentMetadata`](https://github.com/openaire/iis/blob/cdh5/iis-schemas/src/main/avro/eu/dnetlib/iis/metadataextraction/ExtractedDocumentMetadata.avdl) record then expectation is met. Otherwise `UnmetExpectationException` is thrown causing sanity test failure.


