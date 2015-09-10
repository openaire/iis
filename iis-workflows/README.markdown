ICM IIS main parent project which should be used by all KDM modules generating oozie package - it allows for creating separate workflow applications in the inheriting project.

How to use it
-------------
Simply define this module as parent in your `pom.xml` file.
From now on you can:

- build oozie package using oozie profile explicitly, see all the details on:
`https://svn.driver.research-infrastructures.eu/driver/dnet11/modules/icm-iis-core/trunk/docs/oozie-installer.markdown`
- mark integration tests with eu.dnetlib.iis.IntegrationTest what causes all long-run integration tests to be run only on `integration-test` phase
