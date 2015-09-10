"""
.. function:: flow(query:None)

Translates the input query results into sql statements if possible.

:Returned table schema:
    - *query* text
        A complete sql query statement with the semicolon at the end

.. note::

    Input query results must be sql statements separated with semicolons in the first place. Using in the input query the :func:`~functions.vtable.file.file` operator any file with sql statements can be divided in sql query statements. Multiline comments are considered as statements.



Examples:

.. doctest::
    
    >>> sql("select * from (flow file 'testing/testflow.sql') limit 1") # doctest: +NORMALIZE_WHITESPACE
    query
    -----------------------------------------------------------------------------------------------------------------------------------------------------------
    /*====== countries: table of Country ISO codes , country names ===========*/
    CREATE TABLE countries (
        country2 PRIMARY KEY UNIQUE,
        country_name
    );
    >>> sql("select * from (flow file 'testing/colpref.csv' limit 5) ")  #doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    OperatorError: Madis SQLError:
    Operator FLOW: Incomplete statement found : userid colid pr ... 41 416900.0 agr

Test files:

- :download:`testflow.sql <../../functions/vtable/testing/testflow.sql>`
- :download:`colpref.csv <../../functions/vtable/testing/colpref.csv>`



"""
import setpath
import vtbase
import functions
import apsw
import re

registered=True

def filterlinecomment(s):
    if re.match(r'\s*--', s, re.DOTALL| re.UNICODE):
        return ''
    else:
        return s

class FlowVT(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")

        query=dictargs['query']
        connection=envars['db']
        
        yield (('query', 'text'),)
        cur=connection.cursor()
        execit=cur.execute(query, parse = False)

        st=''
        for row in execit:
            strow=filterlinecomment(' '.join(row))
            if strow=='':
                continue
            if st!='':
                st+='\n'+strow
            else:
                st+=strow
            if apsw.complete(st):
                yield [st]
                st=''
        if len(st)>0 and not re.match(r'\s+$', st, re.DOTALL| re.UNICODE):
            if len(st)>35:
                raise functions.OperatorError(__name__.rsplit('.')[-1],"Incomplete statement found : %s ... %s" %(st[:15],st[-15:]))
            else:
                raise functions.OperatorError(__name__.rsplit('.')[-1],"Incomplete statement found : %s" %(st))

def Source():
    return vtbase.VTGenerator(FlowVT)



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


