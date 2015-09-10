This project is a patched version of 

**Cloudera's Avro-JSON proxy for Hadoop Streaming**

The goal of this patch is to introduce a way of passing fully qualified names of classes that define the input and output schemas in the Oozie description. The default approach of this library is to accept either a full schema given in XML property or a path to a file stored somewhere in the filesystem.

See the parent directory of this directory for:
 
 - the original source,
 - scripts for downloading the original source and,
 - scripts for applying the patch in order to create this project.
