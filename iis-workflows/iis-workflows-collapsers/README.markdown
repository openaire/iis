This project contains so-called collapsers and some auxiliary workflow nodes. A **collapser** looks for groups of duplicate objects; when such group is found, it is merged into a single representative object. A collapser workflow node ingests and produces objects conforming to the same schema.

The collapsers that are implemented here use **"blocking" approach** to reduce the computational complexity. This means that in the first step, the input objects are split into separate blocks (groups) where objects with the same value of a certain **blocking key** land in the same block. In the second step, the duplicate objects are searched for and merged within each block only. Objects coming from different blocks are assumed to not be duplicates. Currently, we use a single field of a record as the blocking key (see `GroupByIDMapper` class).

Currently, we have 2 types of collapsers:

- `eu/dnetlib/iis/collapsers/basic_collapser`
- `eu/dnetlib/iis/collapsers/multiple_input_collapser`

See the definitions of workflows of these collapsers for additional info on each of them.
