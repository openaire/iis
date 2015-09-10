"""

.. function:: pipe(query:None[,lines:t])

Executes *query* as a shell command and returns the standard output lines as rows of one column table.
Setting *lines* parameter to *f* the command output will be returned in one table row.

:Returned table schema:
    - *output* text
        Output of shell command execution

Examples::

.. doctest::

    >>> sql("pipe 'ls ./testing/*.csv' ")
    C1
    ---------------------
    ./testing/colpref.csv

    >>> sql("pipe wc ./testing/colpref.csv")
    C1
    ---------------------------------
     19  20 463 ./testing/colpref.csv

.. doctest::
    :hide:
    
    >>> sql("pipe wc nonexistingfile") #doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator PIPE: Command 'wc nonexistingfile' failed to execute because:
    wc: nonexistingfile: No such file or directory
"""


import functions
import vtbase

import subprocess

registered = True
external_stream = True

class PipeVT(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        command = None
        
        if len(largs) > 0:
            command = largs[-1]
        
        if 'query' in dictargs:
            command = dictargs['query']

        if command is None:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No command argument found")
        
        linesplit = True
        if 'lines' in dictargs and dictargs['lines'][0] in ('f', 'F', '0'):
            linesplit = False

        yield (('C1', 'text'),)

        child = subprocess.Popen(command, shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if linesplit:
            pipeiter = iter(child.stdout.readline, '')
            for line in pipeiter:
                yield (line.rstrip("\r\n").decode('utf_8', 'replace'), )
            
            output, error = child.communicate()
        else:
            output, error = child.communicate()

            yield [output.decode('utf_8', 'replace').rstrip("\r\n")]

        if child.returncode != 0:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "Command '%s' failed to execute because:\n%s" %(command,error.rstrip('\n\t ')))

def Source():
    return vtbase.VTGenerator(PipeVT)

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()