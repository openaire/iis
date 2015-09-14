Introduction
============
The workflow description language is placed at the heart of the Information Inference System. It is based on Hadoop's Oozie workflow engine. We aim at developing a kind of wrapper on the Oozie workflow description language that is more suited to our needs which are strictly related to data processing.

This documents describes the idea of the **data workflow description language** and our current approach to creating workflows.

A **more general introduction to the ideas behind the language** are placed in a [Google doc][]. The document contains also an example of the format used by the language.

[Google doc]: https://docs.google.com/document/d/10dfvUV4V0bT2doUTgMi8MUMVkKNATorEg4LCH7RqZaE/edit?usp=sharing

Oozie workflow description and the workflow description language to be introduced in the future
===============================================================================================
The **Oozie workflow description language** is a quite generic workflow language for the Hadoop platform. Because of this, it has no notion nor knowledge of the data passed between workflow nodes. On the other hand, our workflow language will be data processing-oriented and will have a knowledge about the data structures passed from one workflow node to another. In our solution, we will introduce a XML language that describes workflow nodes (a.k.a. processes/processors/modules) and data flow between them; our inspiration is the XML language used in the [Rapid Miner][] open source machine learning software.

[Rapid Miner]: www.rapidminer.com

**In practice**, the definition of the workflow will be a directory containing a XML file describing the workflow and a couple of auxiliary files and subdirectories that define behavior of workflow nodes (`*.jar` files, `*.pig` files, subdirectories for sub-workflows etc.). This definition will be **translated** by a program to an Oozie application, i.e. to an Oozie XML workflow description file along with auxiliary files and subdirectories that define behavior of workflow nodes. The generated Oozie application will conform to certain conventions and will be sent and executed on Hadoop as an Oozie job.

Conventions for the Oozie application
=====================================
Here, we describe **conventions** of the Oozie's `workflow.xml` file that we follow. In future, the file `workflow.xml` conforming to these conventions will be generated automatically from a file describing the data workflow using yet-to-be-developed language.

We assume that each workflow node (or "action" in Oozie's parlance) is defined by a **name that is unique** within bounds of a single workflow (but a subworkflow of this workflow might contain a workflow node with the same name as some workflow node in this workflow). It is defined in `<action name="WORKFLOW_NODE_NAME">` tag. The name should be a **valid HDFS file/directory name**.

All the data produced by a given workflow node is stored in a directory having the same name as the name of the workflow node. For example, for a node "sample_processor" that produces two output data stores with names "processed_person", "processed_document" we have the following **directory structure**:

	sample_processor/
	|
	|- working_dir/
	|- processed_person/
	\- processed_document/


To this aim, the workflow engine passes a couple of **paths** to each workflow node (or "actions" in Oozie's parlance):

- Path to the **working directory** (denoted as `workingDir` in the Oozie XML file). This is the place in which the node can store results of its intermediate computations, temporary files etc. In case of this node being a subworkflow, this is the place where directories of its child workflow nodes are stored. This directory is named `working_dir` in the example directory structure above.
- Paths to **input and output data stores**:
	- A single **data store** is a directory containing one or more [Avro Object Container Files][]. Each of these files has the same schema. All the records stored in these files make up the contents of the data store. Note that these files might not contain any records, in such case we say that the data store is "empty".
	- It will be the responsibility of the translator that translates our workflow definition to Oozie application to make sure that **data given as an input to a workflow node is compatible** with the data the node accepts. For example, if the node accepts a structure `Person` with fields `int id`, `string name`, `string surname`, then the structure passed to that node has to have the same name (here: `Person`) and have at least the mentioned fields. It can have more fields, but these are mandatory.
	- The paths to the input data stores reference output data stores produced by some other workflow nodes.
- The "sample_processor" directory is recreated in the `<prepare>` section of the Oozie's action. This is to ensure that the workflow can be properly restarted in case of transient errors and possibly stopped and resumed (I'm not sure about the latter two -- this has to be checked in practice).

[Avro Object Container Files]: http://avro.apache.org/docs/1.7.4/spec.html#Object+Container+Files

Each workflow node can also be passed a **set of properties**, i.e. (key, value) string pairs. These properties are passed possibly in different ways - the way of passing them depends on the type of the workflow node (there's one mechanism to pass it to Java workflow node and some other to pass it to Java map-reduce job workflow node). These properties can be accessed by the code implementing the workflow node.

Each workflow node should somehow **provide an information about the types accepted by its input ports and produced by its output ports**. These information should be eventually provided in a form of Avro JSON schemas. The JSON schemas don't have to be provided directly, i.e. as explicit schemas in the body of the workflow; they can be provided, e.g. as a link to JSON file storing the schema or a name of Java class that produces the schema (as it is done currently in case of IIS'es Java workflow nodes)

When implementing new type of workflow node
-------------------------------------------
To sum up, when implementing a new type of workflow node, these are the things that your implementation has to provide:

- a possibility to **retrieve type of the input and output ports**
- **passing parameters** to the workflow node
- executing the node **in a sub-workflow**
- running the **integration tests** with this kind of node (if possible)

Current state of implementation
===============================
Currently we don't have the format of the XML workflow definition file defined, as a consequence we also don't have the translator program defined. Because of this, when creating a new workflow or implementation of workflow node, we have to **conform to the described conventions**. You can look at the source code of example workflows in the `iis-core-examples` module for examples of how workflows and workflow nodes are defined. Conforming to the conventions is important, since in future we would like to convert these workflows to the (not yet available) XML workflow definition file format.

Currently (as of 2013-03-29), examples of the following workflow nodes conforming to these conventions are **available**:

- Java node
- subworkflow node
- map-reduce node (partially, since more advanced examples are missing)

these are **planned** to be prepared (in order of priority, more or less):

- Pig node
- parallel execution node
- conditional node
- Oozie node
- webservice node
- error handling functionality
- Python node
