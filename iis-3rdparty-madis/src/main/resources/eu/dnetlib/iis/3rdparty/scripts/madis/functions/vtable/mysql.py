"""
.. function:: mysql(host, port, user, passwd, db, query:None)

Connects to an MySQL DB and returns the results of query.

Examples:

    >>> sql("select * from (mysql h:127.0.0.1 port:3306 u:root p:rootpw db:mysql select 5 as num, 'test' as text);")
    num | text
    -----------
    5   | test

"""

import setpath
import vtbase
import functions
from types import NoneType

registered=True

class MySQL(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        from lib import pymysql
        t = pymysql.FIELD_TYPE
        
        typetrans = {
            t.DECIMAL:'INT',
            t.TINY: 'INT',
            t.SHORT: 'INT',
            t.LONG: 'INT',
            t.FLOAT: 'REAL',
            t.DOUBLE: 'REAL',
            t.NULL: '',
            t.TIMESTAMP: 'TEXT',
            t.LONGLONG: 'TEXT',
            t.INT24: 'TEXT',
            t.DATE: 'TEXT',
            t.TIME: 'TEXT',
            t.DATETIME: 'TEXT',
            t.YEAR: 'INT',
            t.NEWDATE: 'TEXT',
            t.VARCHAR: 'TEXT',
            t.BIT: 'INT',
            t.NEWDECIMAL: 'INT',
            t.ENUM: 'TEXT',
            t.SET: 'TEXT',
            t.TINY_BLOB: 'TEXT',
            t.MEDIUM_BLOB: 'TEXT',
            t.LONG_BLOB: 'TEXT',
            t.BLOB: 'TEXT',
            t.VAR_STRING: 'TEXT',
            t.STRING: 'TEXT',
            t.GEOMETRY: 'TEXT'
        }

        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")

        query=dictargs['query']

        host = dictargs.get('host', dictargs.get('h', '127.0.0.1'))
        port = int(dictargs.get('port', 3306))
        user = dictargs.get('user', dictargs.get('u', 'root'))
        passwd = dictargs.get('passwd', dictargs.get('p', ''))
        db = dictargs.get('db', 'mysql')

        try:
            conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, use_unicode = True)

            cur = conn.cursor(pymysql.cursors.SSCursor)
            cur.execute(query)

            desc = cur.description
            if desc == None:
                yield [( 'None', )]
            else:
                yield [( c[0], typetrans.get(c[1], '') ) for c in desc]

            for i in cur:
                yield [unicode(c) if type(c) not in (long, int, float, str, unicode, NoneType, bool) else c for c in i]

        except (pymysql.err.InternalError, pymysql.err.ProgrammingError) as e:
            raise functions.OperatorError(__name__.rsplit('.')[-1], str(e[0]) +': ' + e[1])
        except Exception, e:
            raise functions.OperatorError(__name__.rsplit('.')[-1], str(e))
        finally:
            try:
                conn.close()
            except:
                pass

        
def Source():
    return vtbase.VTGenerator(MySQL)


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