"""
.. function:: oracle(jdbc_connection_string, user, passwd, query:None)

Connects to an Oracle DB and returns the results of query.

The requirements for this to work are:

 - Install JPype Python package:

    Homepage: http://jpype.sourceforge.net/

    In Debian based systems such as Ubuntu, install python-jpype using:

        sudo apt-get install python-jpype

 - Add the Oracle JDBC JAR (ojdbc5.jar) in directory madis/lib/jdbc/.

 - Have a JVM installed, and the JAVA_HOME environment variable set correctly.

Examples:

    >>> sql("select * from (oracle jdbc:oracle:thin:@//127.0.0.1:6667/xe u:system p:password select 5 as num, 'test' as text);")
    num | text
    -----------
    5   | test

"""

import setpath
import vtbase
import functions

registered=True

class Oracle(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        from lib import jaydebeapi
        import os
        from types import NoneType
        try:
            import jpype
        except ImportError:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"""
For this operator to work you'll need to:
 - Install JPype Python package. For Debian/Ubuntu do:
     sudo apt-get install python-jpype
 - Add the Oracle JDBC JAR (ojdbc5.jar) in directory madis/lib/jdbc/.
 - Have a JVM installed, and the JAVA_HOME environment variable set correctly.
        """)

        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No query argument ")

        query=dictargs['query']
        jdbc = 'jdbc:' + str(dictargs.get('jdbc', 'oracle:thin:@//127.0.0.1:6667/xe'))
        user = str(dictargs.get('user', dictargs.get('u', 'system')))
        passwd = str(dictargs.get('passwd', dictargs.get('p', '')))
        jar = str(dictargs.get('jar', 'ojdbc5.jar'))

        if os.path.split(jar)[0] == '':
            jarpath = os.path.dirname(os.path.abspath(__file__))
            jar = os.path.abspath(os.path.join(jarpath, '..', '..', 'lib', 'jdbc', jar))

        try:
            conn = jaydebeapi.connect('oracle.jdbc.pool.OracleDataSource', [jdbc, user, passwd], jar,)
            cur = conn.cursor()
            cur.execute(query)
            yield [(c[0], c[1]) for c in cur.description]

            while True:
                row = cur.fetchone()
                if not row:
                    break
                yield [unicode(c) if type(c) not in (long, int, float, str, unicode, NoneType, bool) else c for c in row]

            cur.close()

        except Exception, e:
             raise functions.OperatorError(__name__.rsplit('.')[-1], ' '.join(str(t) for t in e))


def Source():
    return vtbase.VTGenerator(Oracle)


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