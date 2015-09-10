"""

.. function:: jsonpipe(query:None[,lines:t])

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
import itertools
import json
import subprocess

registered = True
external_stream = True

class JSONPipeVT(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        command = None
        
        if len(largs) > 0:
            command = largs[-1]
        
        if 'query' in dictargs:
            command = dictargs['query']

        if command is None:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No command argument found")
        
        child = subprocess.Popen(command, shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        jsondecode = json.JSONDecoder().scan_once
        pipeiter = iter(child.stdout.readline, '')
        firstline = ''

        try:
            firstline = pipeiter.next()
        except StopIteration:
            yield (('C1', 'text'),)
            raise StopIteration
            return

        namelist = []
        schemaline = json.loads(firstline)
        schemalinetype = type(schemaline)

        if schemalinetype == list:
            for i in xrange(1, len(schemaline)+1):
                namelist.append( ['C'+str(i), 'text'] )
            pipeiter = itertools.chain([firstline], self.fileiter)
        elif schemalinetype == dict:
            namelist += schemaline['schema']
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "Input file is not in line JSON format")

        yield tuple(namelist)

        if "MSPW" in functions.apsw_version:
            for line in pipeiter:
                yield json.loads(line)
        else:
            for line in pipeiter:
                yield jsondecode(line, 0)[0]

        output, error = child.communicate()

        if child.returncode != 0:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "Command '%s' failed to execute because:\n%s" %(command,error.rstrip('\n\t ')))

def Source():
    return vtbase.VTGenerator(JSONPipeVT)

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