Description of the project
--------------------------
The role of this project is to deliver **basic generic building blocks** to create Oozie-based workflows consisting of various types of workflow nodes. Namely, it provides:

- A standardized **way of defining workflow applications**. Currently it only defines certain conventions that the Oozie workflow XML definition must conform to (see `docs/workflow_engine_description.markdown`), but in the future it is supposed to define a data workflow definition language that will be used to generate Oozie workflow applications conforming to these conventions. Apart from that it also provides a Maven-based tool to create Oozie workflow application packages (see `docs/oozie-installer.markdown`).
- A set of **generic workflow nodes of various types**. To be more precise, it contains code that might or must be used by nodes of these types.


Other
-----
- See the `docs` directory for information about the installation of the system, how to create a workflow applcation package to be executed through Oozie etc.
- See the `src/main/scripts` directory for some useful tools.
