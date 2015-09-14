About
-----
This is the main parent project which should be used by all projects that define Oozie workflows - it allows for creating separate workflow applications in the inheriting project.

How to use it
-------------
Simply define this module as parent in your `pom.xml` file.

From now on you can:

- build oozie package using oozie profile explicitly, see all the details in `docs/oozie-installer.markdown`
- mark integration tests with eu.dnetlib.iis.IntegrationTest what causes all long-run integration tests to be run only on `integration-test` phase

Documents
---------
See the `docs` directory for:

- information on how to create workflow application packages to be executed by Oozie using our Maven-based tool,
- conventions to be used when defining Oozie workflows.
