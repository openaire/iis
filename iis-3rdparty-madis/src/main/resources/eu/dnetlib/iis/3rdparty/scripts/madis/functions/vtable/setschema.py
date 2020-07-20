"""

.. function:: setschema(query:None,schemadefinition)

    Returns the result of the input *query* with changed schema according to *schemadefinition* parameter.
    Parameter *schemadefinition* is text identical to schema definition between parenthesis of a CREATE TABLE SQL statement.
    
    Can perform renaming, typecasting and projection on some columns of the input *query* result.

.. note::

    This function can be used to avoid DynamicSchemaWithEmptyResultError caused by dynamic schema virtual tables on empty query input result.

    .. toadd link.
    
    

:Returned table schema:
    As defined at *schemadefinition* parameter.


Examples::

    >>> sql("setschema 'col1 int,col2 text' select 5,6")
    col1 | col2
    -----------
    5    | 6
    
    >>> sql("select strsplitv(q) from (select 5 as q) where q!=5")    #doctest:+ELLIPSIS +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    DynamicSchemaWithEmptyResultError: Madis SQLError:
    Operator EXPAND: Cannot initialise dynamic schema virtual table without data

    >>> sql("setschema 'a,b' select strsplitv(q) from (select 5 as q) where q!=5")
    a | b
    -----

    >>> sql("select * from (file file:testing/colpref.csv dialect:csv header:t) limit 3")
    userid | colid | preference | usertype
    --------------------------------------
    agr    |       | 6617580.0  | agr
    agr    | a0037 | 2659050.0  | agr
    agr    | a0086 | 634130.0   | agr

The query below has constraints preference column to be less than an int value , but preference is text ( outcomes from :func:`~functions.vtable.file.file` are *text*), so an empty result is produced
    
    >>> sql("select * from (select * from (file file:testing/colpref.csv dialect:csv header:t) limit 3) where cast(preference as int) <634130")

With setschema functions preference column is casted as float.
    
    >>> sql("select * from (setschema 'type,colid , pref float, userid' select * from (file file:testing/colpref.csv dialect:csv header:t) limit 3) where pref<634131")
    type | colid | pref     | userid
    --------------------------------
    agr  | a0086 | 634130.0 | agr

"""
from lib.sqlitetypes import typestoSqliteTypes
import vtbase
import functions
from lib.pyparsing import Word, alphas, alphanums, Optional, Group, delimitedList, quotedString , ParseBaseException

registered=True

ident = Word(alphas+"_",alphanums+"_")
columnname = ident | quotedString
columndecl = Group(columnname + Optional(ident))
listItem = columndecl

def parsesplit(s):
    global listItem
    return delimitedList(listItem).parseString(s,parseAll=True).asList()

def checkexceptionisfromempty(e):
    e=str(e).lower()
    if 'no' in e and 'such' in e and 'table' in e and 'vt_' in e:
        return True
    return False

class SetSchema(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        """
        Works only with one argument splited with ,,,,
        """

        largs, dictargs = self.full_parse(parsedArgs)

        names = []
        types = []

        if len(largs)<1:
            raise functions.OperatorError(__name__.rsplit('.')[-1]," Schema argument was not provided")
        try:
            schema=parsesplit(largs[0])
        except ParseBaseException:
            raise functions.OperatorError(__name__.rsplit('.')[-1]," Error in schema definition: %s" %(largs[0]))
        for el in schema:
            names.append(el[0])
            if len(el)>1:
                types.append(el[1])
            else:
                types.append('')

        query = dictargs['query']
        c=envars['db'].cursor()

        ### Find names and types
        execit=c.execute(query)
        qtypes=[str(v[1]) for v in c.getdescriptionsafe()]

        for i in xrange(len(types)):
            if types[i]=='' and i<len(qtypes) and qtypes[i]!='':
                types[i]=qtypes[i]

        yield [(i,j) for i,j in zip(names,types)]

        sqlitecoltype=[typestoSqliteTypes(type) for type in types]

        namelen = len(names)
        for row in execit:
            row = row[:namelen] + (None,) * (namelen - len(row))
            ret =[]
            for i,val in enumerate(row):
                e=val
                if sqlitecoltype[i] in ("INTEGER", "REAL", "NUMERIC"):
                    try:
                        e=int(val)
                    except ValueError:
                        try:
                            e=float(val)
                        except ValueError:
                            e=val
                ret+=[e]
            yield ret

def Source():
    return vtbase.VTGenerator(SetSchema)

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



