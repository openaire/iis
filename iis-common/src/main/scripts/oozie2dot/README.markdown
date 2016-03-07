This directory contains scripts to convert Oozie XML workflow description files to their graphical representation in Graphviz `*.dot` files and `*.png` files. By doing that, you can easily visualize a complicated workflow defined in the XML file.

Contents:

- `scripts` directory - contains scripts that convert Oozie XMLs to `*.png` files
- Execute `run_example.sh` to see how the scripts works on real-life-like workflow XML descriptions

Requirements:

- The `oozie2png.sh` and `ooziedir2png.sh` scripts requires the `Graphviz` programs (`dot` program do be more precise) to be installed in the system.

Related projects:

- After implementing these scripts, I learned that there are other projects already available that convert Ooize workflow XML definition to Graphviz (Oops!):
    - https://github.com/iprovalo/vizoozie
    - https://github.com/thedatachef/lookoozie
