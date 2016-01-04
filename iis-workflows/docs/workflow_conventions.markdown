# Introduction

**Abstract**. This document describes the conventions that we use when defining Oozie workflows.

**Data processing pipeline**. Each Oozie workflow describes an imperative implementation of a higher-level declarative data processing pipeline. The pipeline representation follows **"Pipes and filters"** pattern from the book by Gregor Hohpe, Bobby Woolf: "Enterprise Integration Patterns: Designing, Building, and Deploying Messaging Solutions", Addison-Wesley, 2003. The pipeline consists of workflow **nodes**, known as "actions" in Oozie parlance, that consume data arriving on their **input ports** and produce data to their **output ports**. Each port has a name and a type (defined by Avro schema). Output port of a producer is connected to the input port of a consumer; in order for this connection to work properly, the types of these ports have to "match". The data passed between the nodes is called **data store** and in practice this is simply a directory containing one or more [Avro Object Container Files][]. This document describes mostly how this high-level model is implemented in practice in Oozie workflows used in the project.

**Data processing language**. Note that the original idea was that Oozie workflows conforming to the conventions described in this document would be automatically generated from a description of data processing pipeline. However, implementation of such description language has not been a priority and as of 2015-12-22 probably it won't be done. You can read a description of this language in a [Google doc][].

[Google doc]: https://docs.google.com/document/d/10dfvUV4V0bT2doUTgMi8MUMVkKNATorEg4LCH7RqZaE/edit?usp=sharing

# General naming conventions

Use `words_separated_by_underscores` to name actions, parameters, etc. instead of, e.g. `camelCase`.

# Ports

## Standard ports

### All Oozie actions except Java actions

#### Names

For all actions except for Java actions, **names** of properties that correspond to ports have to be prefixed properly:

- input ports: `input_`, e.g. `input_document`;
- output_ports: `ouput_`, e.g. `output_person`.

One can also use a property named simply `input` or `output`, which means that the name of the port is simply `input` or `output`, respectively. In such case, no other input or output ports, should be defined for given node.

These rules also apply to `parameters` section in a definition of a workflow, i.e. this section might contain definitions of input and output ports of the workflow.

#### Schemas

In case of properties correspoding to **schemas of ports**, their names should be prefixed with `schema_input_` or `schema_output_`, depending on the type of the port. The value should be a fully qualified name of the schema. For example, take a look at the following (property name, path) pair corresponding to a property:

    (schema_input_text, eu.dnetlib.iis.metadataextraction.schemas.DocumentText)


The properties corresponding to schemas of ports should be placed next to the properties corresponding to the ports themselves, e.g.

    <param>input_metadata=${transformerCitationmatchingInputMetadata}</param>
    <param>schema_input_metadata=eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal</param>

    <param>input_person=${transformerCitationmatchingInputPerson}</param>
    <param>schema_input_person=eu.dnetlib.iis.importer.schemas.Person</param>


Property named `schema_input` or `schema_output` corresponds to `input` or `output` port property, respectively.

Property named `schema` means a schema corresponding to all input and output ports.

### Java actions

In case of Java actions, ports are given as command line arguments and prefixed properly:

- input ports: `-I`, e.g. `-Idocument`;
- output ports: `-O`, e.g., `Odocument`.


## Root ports

There is an alternative way of defining nodes and paths to data stores - so-called **root ports**. A single port is defined through two properties, one being the "root" and the other one being the "leaf". For example, in the following (property name, path) pairs of properties

    (output_person_root, X), (person_output_name_age, Y)

the former entry is the root while the latter is the leaf. This corresponds to the following (property name, path) pair from the standard port notation: 

    (output_person_age,  X/Y)

This notation is necessary when using Java MapReduce workflow nodes with multiple outputs.

# Workflow name

The name of the workflow should correspond to the path leading to this workflow in the code repository after removing constant prefix `eu/dnetlib/iis` and removing constant suffix `oozie_app/workflow.xml`. For example, if the path to the workflow is `eu/dnetlib/iis/transformers/citationmatching/oozie_app/workflow.xml`, the entry in the Oozie XML should be

    <workflow-app xmlns="uri:oozie:workflow:0.2" name="transformers_citationmmatching">.

where the slashes in the path are replaced with underscores (`_`). This approach assures that the names of workflows are both unique and quite short.

In case of a test workflow, its name should be created in the same way as the normal workflow but it should be prefixed additionally with `test-`, e.g. the test workflow placed at `eu/dnetlib/iis/transformers/citationmatching/sampledataproducer/oozie_app/workflow.xml` should be called `test-transformers_citationmatching_sampledataproducer`.

# Special types of Oozie actions

We came to use some conventions related to names of actions:

- Prefixed with `skip-` - a node that is used to generate empty data store as an alternative to some "real" processing.
- Prefixed with `transformers_` or `transformer_` - a node that converts the data stores to a format expected by their consumers.
- Suffixed with `_collapser` - a node that uses "collapser" infrastructure to de-duplicate records from data store.
- Suffixed with `_union` - a node that executes a "union" operation on data stores.

# Directory structure corresponding to Oozie action

The **names of Oozie actions** should be valid HDFS file/directory names. This is because a directory named as the action is created in HDFS for most actions.

By default, all data produced by a given workflow node is stored in a directory having the same name as the name of the workflow node. For example, for a node "sample_processor" that produces two output data stores with names "processed_person", "processed_document" we have the following **directory structure**:

    sample_processor/
    |
    |- working_dir/
    |- processed_person/
    \- processed_document/


Here:

- `workingDir` corresponds to a directory where the node can store results of its intermediate computations, temporary files etc. In case of this node being a subworkflow, this is the place where directories of its child workflow nodes are stored. This directory is named `working_dir` in the file system.
- In Oozie definition, `working_dir` and output data store directories are removed in the `<prepare>` XML section to ensure that the action can be restarted whenever an unexpected error occurs. Note we cannot just remove `sample_processor` directory to achieve this goal because output directories may be defined by a workflow calling this workflow (see below) as residing outside given node directory structure.

In case when workflow A calls workflow B, the **paths to directories where B saves its output data stores are passed from workflow A as parameters**. Then for certain workflow nodes in B, the output data store directories won't be subdirectories of the directory corresponding to this workflow node.

[Avro Object Container Files]: http://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files
