This directory and its subdirectories and files are here as a hack to make the Oozie unit tests work. 

Details
-------
Oozie tests assume that they're placed inside directory tree of Oozie source code -- see the source code of class `XTestCase` which is an ancestor of `MiniOozieTestCase` class which, in turn, should be inherited by your test case class. 

How to get the source code of the `XTestCase` class:

- download source code of the Ubuntu's `oozie` package prepared by Cloudera (`apt-get source oozie`). It is version 3.1.3+155 of this package. 
- open file `oozie-3.1.3+155/src/core/src/test/java/org/apache/oozie/test/XTestCase.java` and look at lines 93-105.
