# Rules of matching by hand

A given affiliation is considered to match a given organization if the 
person that matches them decides so by using all the knowledge they have 
and by using all accessible sources of information (mainly the Internet).

It is worth noting that the institutions and their departments are not 
linked in any way in the OpenAire organization database. However, an 
individual that prepares the quality test data takes the relations 
between an institution and its departments into account. In particular:

* If a given institution in a given affiliation is a department of a
  certain organization then it will be matched with this particular
  department (if it is in the organization database) AND with the main
  organization (if it is in the organization database)

  For example:

  * Affiliation A => "organization": "Interdisciplinary Centre for Mathematical and Computational Modelling, University of Warsaw"
  * Organization database:
      * Organization A => "name": "Interdisciplinary Centre for Mathematical and Computational Modelling, University of Warsaw"
      * Organization B => "name": "University of Warsaw"

  The affiliation A will be matched with both the organization A and B.

* If a given institution in a given affiliation is the main
  organization (in its hierarchy) then it will only be matched with this
  particular organization (if it is in the organization database)

  For example:

  * Affiliation A => "organization": "University of Warsaw"
  * Organization database:
      * Organization A => "name": "Interdisciplinary Centre for Mathematical and Computational Modelling, University of Warsaw"
      * Organization B => "name": "University of Warsaw"

  The affiliation A will only be matched with the organization B.

* (HOWEVER,) If a given institution in a given affiliation is the main
  organization (in its hierarchy) but the publication is assigned to
  the same project as one of departments of this organization then the
  affiliation will be matched with the main organization (if it is in
  the organization database) and with the department that is assigned to
  the project.

  For example:

  * Affiliation A => "organization": "University of Warsaw"
  * Organization database:
      * Organization A => "name": "Interdisciplinary Centre for Mathematical and Computational Modelling, University of Warsaw"
      * Organization B => "name": "University of Warsaw"

  The publication is assigned to the same project as the organization A.

  The affiliation A will be matched with both the organization A and B.

* If a given institution in a given affiliation is not a department of
  some organization even if its name suggests otherwise, then it will
  only be matched with the this particular organization (if it is in the
  organization database).

  For example:

  * Affiliation A => "organization": "Radboud University Nijmegen Medical Centre"
  * Organization database:
      * Organization A => "name": "Radboud University Nijmegen"
      * Organization B => "name": "Radboud University Nijmegen Medical Centre"

  The affiliation A will only be matched with the organization B (Radboud 
  University Nijmegen Medical Centre is not a part of Radboud University 
  Nijmegen).
