"""
.. function:: exec(query:None,[path:None,variables])

Executes the input query. Gets the first column of the returned result and executes its rows content supposing it is an sql statement. *Path* parameter sets the current working directory while executing the statements.
*Variables* are named parameters that set variables in execution environment. For example *c:v* named parameter sets the variable *c* in the new environment, initialized with current variable's *v* value.

:Returned table schema:
    - *return_value* int
        Boolean value 1 indicating success of the SQL statements flow execution. On failure an exception is thrown.

.. toadd See also variables.. LINK , file???

Examples:

.. doctest::
    :hide:
    
    >>> settestdb('../../tests/temp.db3')

This query executes the statements in quotes and returns successfully

.. doctest::

    >>> sql("exec select 'select 5'")
    return_value
    ------------
    1

Typical usage.

.. doctest::

    >>> sql("file 'testing/testtable.sql'")
    C1
    ----------------------------------------
    create table table1 (a,b,c);
    insert into table1 values('James',10,2);
    insert into table1 values('Mark',7,3);
    insert into table1 values('Lila',74,1);

    >>> sql("exec file 'testing/testtable.sql'")
    return_value
    ------------
    1
    
    >>> sql("select * from table1")
    a     | b  | c
    --------------
    James | 10 | 2
    Mark  | 7  | 3
    Lila  | 74 | 1

Nesting flows. Usage of *path* and variables parameters.

.. doctest::

    >>> sql("file 'testing/topflow.sql'")
    C1
    -----------------------------------------------------------------------------
    var 'v' 5;
    var 'tablename' 'internaltable';
    var 'lastdate' from select '2008-01-01';
    create table topflowvars as select * from getvars() where variable!='execdb';
    exec 'tablename' 'c:v'  file 'internalflow.sql';

    >>> sql("file 'testing/internalflow.sql'")
    C1
    ------------------------------------------------------------------------------
    create table %{tablename} as select * from getvars() where variable!='execdb';

    >>> sql("select * from getvars() where variable!='execdb'")
    variable | value
    ----------------
    flowname |

    >>> sql("exec 'path:testing' file 'testing/topflow.sql'")
    return_value
    ------------
    1
    >>> sql("select * from topflowvars")
    variable  | value
    -------------------------
    flowname  | notset
    lastdate  | 2008-01-01
    tablename | internaltable
    v         | 5

    >>> sql("select * from internaltable")
    variable  | value
    -------------------------
    flowname  | notset
    c         | 5
    tablename | internaltable

    >>> sql("select * from getvars() where variable!='execdb'")
    variable | value
    ----------------
    flowname |

.. doctest::
    :hide:
    
    >>> import os
    >>> os.remove('../../tests/temp.db3')

Test files:

- :download:`testtable.sql <../../functions/vtable/testing/testtable.sql>`
- :download:`topflow.sql <../../functions/vtable/testing/topflow.sql>`
- :download:`internalflow.sql <../../functions/vtable/testing/internalflow.sql>`

"""

import copy
import os.path
    
import setpath          #for importing from project root directory  KEEP IT IN FIRST LINE
from vtout import SourceNtoOne
import functions
import logging
import datetime
import os
import copy
import re
import time
import types

comment_line=re.compile(r'/\*.*?\*/(.*)$')
registered=True

def execflow(diter, schema, connection,*args,**kargs):
    ignoreflag='ignorefail'

    if functions.variables.execdb==None:
        con=functions.Connection('')
    else:
        con=functions.Connection(functions.variables.execdb)
    
    functions.register(con)
    oldvars=functions.variables
    newvars=lambda :x
    newpath=None
    path=os.getcwd()

    if 'path' in kargs:
        newpath=os.path.abspath(kargs['path'])
        del kargs['path']
        os.chdir(newpath)

    newvars.execdb=functions.variables.execdb
    newvars.flowname='notset'
    for v in args:
        if hasattr(functions.variables,v):
            newvars.__dict__[v]=functions.variables.__dict__[v]
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Variable %s doen't exist" %(v))
    for newv,oldv in kargs.items():
        if hasattr(functions.variables,oldv):
            newvars.__dict__[newv]=functions.variables.__dict__[oldv]
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Variable %s doen't exist" %(oldv))
    functions.variables=newvars

    if functions.settings['logging']:
        lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
        lg.info("############FLOW START###################")
    before=datetime.datetime.now()

    try:

        line=0
        for query in diter:
            
            if len(query)>1:
                raise functions.OperatorError(__name__.rsplit('.')[-1],"Ambiguous query column, result has more than one columns")
            line+=1
            if type(query[0]) not  in types.StringTypes:
                raise functions.OperatorError(__name__.rsplit('.')[-1],"Content is not sql query")
            #Skip empty queries or comment lines
            query=query[0].strip()
            if query.startswith("--"):
                continue
            cmatch=comment_line.match(query)
            if query=='' or (cmatch!=None and cmatch.groups()[0]==''):
                continue

            if functions.settings['logging']:
                lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
                lg.info("STARTING: %s" %(query))
            before=datetime.datetime.now()
            c=con.cursor()
            # check ignore flag
            catchexception=False
            if query.startswith(ignoreflag):
                catchexception=True
                query=query[len(ignoreflag):]
            try:
                for i in c.execute(query):
                    pass
            except Exception,e: #Cathing IGNORE FAIL EXCEPTION          
                if catchexception:
                    if functions.settings['logging']:
                        lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
                        lg.exception("Ignoring Exception: "+str(e))
                    continue
                else:
                    try:
                        c.close()
                        c=con.cursor()
                        c.execute('rollback')
                    except:
                        pass
                    raise e

            if functions.settings['logging']:
                lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
                after=datetime.datetime.now()
                tmdiff=after-before
                duration="%s min. %s sec %s msec" %((int(tmdiff.days)*24*60+(int(tmdiff.seconds)/60),(int(tmdiff.seconds)%60),(int(tmdiff.microseconds)/1000)))
                lg.info("FINISHED in %s: %s" %(duration,query))
            c.close()

    except Exception,e:
        if functions.settings['logging']:
            lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
            lg.exception(e)
        raise functions.OperatorError(__name__.rsplit('.')[-1],"Error in statement no. %s query '%s':\n%s" %(line,query,str(e)))
    finally:
        try:
            con.close()
        except:
            pass
        after=datetime.datetime.now()
        tmdiff=after-before
        fltm="Flow executed in %s min. %s sec %s msec" %((int(tmdiff.days)*24*60+(int(tmdiff.seconds)/60),(int(tmdiff.seconds)%60),(int(tmdiff.microseconds)/1000)))
        if functions.settings['logging']:
            lg = logging.LoggerAdapter(logging.getLogger(__name__),{ "flowname" : functions.variables.flowname  })
            lg.info(fltm)
            lg.info("#############FLOW END####################")
        functions.variables=oldvars
        if newpath:
            os.chdir(path)

def Source():
    return SourceNtoOne(execflow,connectionhandler=True)


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
        
