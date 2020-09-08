"""
.. function:: queryplan(query) -> Query plan

Returns the query plan of the input query.

Examples::

    >>> sql("queryplan select 5")
    operation     | paramone | paramtwo | databasename | triggerorview
    ------------------------------------------------------------------
    SQLITE_SELECT | None     | None     | None         | None

"""

import setpath
import vtbase
import functions
import apsw

registered=True

class QueryPlan(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        def authorizer(operation, paramone, paramtwo, databasename, triggerorview):
            """Called when each operation is prepared.  We can return SQLITE_OK, SQLITE_DENY or
            SQLITE_IGNORE"""
            # find the operation name
            plan.append([apsw.mapping_authorizer_function[operation], paramone, paramtwo, databasename, triggerorview])
            return apsw.SQLITE_OK

        def buststatementcache():
            c = connection.cursor()
            for i in xrange(110):
                a=list(c.execute("select "+str(i)))

        _, dictargs = self.full_parse(parsedArgs)
        
        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1]," needs query argument ")

        query=dictargs['query']

        connection = envars['db']
        plan=[]

        buststatementcache()

        cursor = connection.cursor()

        cursor.setexectrace(lambda x,y,z:apsw.SQLITE_DENY)

        connection.setauthorizer(authorizer)

        cursor.execute(query)

        connection.setauthorizer(None)

        yield [('operation', 'text'), ('paramone', 'text'), ('paramtwo', 'text'), ('databasename', 'text'), ('triggerorview', 'text')]

        for r in plan:
            yield r
    
    def destroy(self):
        pass

def Source():
    return vtbase.VTGenerator(QueryPlan)

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
