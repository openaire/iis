Description of the project
--------------------------
This project is tightly integrated with `iis-core` project. It contains **examples of usage of various types of workflow nodes** defined in `iis-core`. A single example is supposed to serve as a reference/template for a person implementing their own concrete workflow node of a certain type. By the way, the examples are also unit/integration tests, so we know that they work ;)

Workflow tests using Oozie testing facilities
---------------------------------------------
This project contains tests of various types of workflow nodes. If you want to **implement analogous workflow tests in some other Maven project**, your test case class will have to inherit from the `iis-core`'s ` eu.dnetlib.iis.core.AbstractWorkflowTestCase` class. Please read the description given in the javadoc of `eu.dnetlib.iis.core.AbstractWorkflowTestCase` class for more details and see other test case classes in this project for example workflow tests. 

Other
-----
See the `src/main/scripts` directory for sample scripts that build Oozie workflow packages.
