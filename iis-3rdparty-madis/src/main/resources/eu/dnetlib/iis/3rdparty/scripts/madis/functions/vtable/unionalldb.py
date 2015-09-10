"""
.. function:: unionalldb(db_filename)

This function returns the contents of a table that has been split using OUTPUT split functionality.

Its input are DBs with names such as:

  dbname.0.db
  dbname.1.db
  ...

It is assumed that inside each of above DBs, a table named as *dbname* will exist. All of these
tables should have the same schema

If a *start* or *end* argument is present then *unionalldb* will start scanning from the *start* numbered part
and end scanning at *end* numbered db part (without including the *end* numbered part).

Usage examples:

  select * from (unionalldb 'dbname');

  select * from (unionalldb start:1 end:4 'dbname');

"""
import vtbase
import functions
import apsw
import os
import sys
import gc

registered=True

class UnionAllDB(vtbase.VT):
    def findschema(self):
        try:
            # Try to get the schema the normal way
            schema = self.xcursor.getdescription()
        except apsw.ExecutionCompleteError:
            # Else create a tempview and query the view
            list(self.xcursor.execute('create temp view temp.___schemaview as '+ self.query + ';'))
            schema = [(x[1], x[2]) for x in list(self.xcursor.execute('pragma table_info(___schemaview);'))]
            list(self.xcursor.execute('drop view temp.___schemaview;'))

        return schema

    def VTiter(self, *parsedArgs,**envars):
        opts=self.full_parse(parsedArgs)

        self.query=None

        self.start = 0
        self.end = sys.maxint

        if 'start' in opts[1]:
            self.start = int(opts[1]['start'])

        if 'end' in opts[1]:
            self.end = int(opts[1]['end'])

        try:
            dbname=opts[0][0]
        except:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"A DB filename should be provided")

        try:
            self.query=opts[1]['query']
        except:
            pass

        self.dbfile = str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(dbname)))))

        tablename = os.path.split(self.dbfile)[1]
        if 'tablename' in opts[1]:
            tablename=opts[1]['tablename']

        if 'table' in opts[1]:
            tablename=opts[1]['table']

        if self.query == None:
            self.query = 'select * from '+tablename+';'

        self.part = self.start
        try:
            self.xcon=apsw.Connection(self.dbfile+'.' + str(self.part) + '.db', flags=apsw.SQLITE_OPEN_READONLY)
        except Exception,e:
            print e
            raise functions.OperatorError(__name__.rsplit('.')[-1],"DB could not be opened")

        self.xcursor=self.xcon.cursor()
        self.xexec=self.xcursor.execute(self.query)
        yield self.findschema()

        while self.part < self.end:
            try:
                self.xcon = apsw.Connection(self.dbfile+'.' + str(self.part) + '.db', flags=apsw.SQLITE_OPEN_READONLY)
                self.xcursor = self.xcon.cursor()
                self.xexec =self.xcursor.execute(self.query)
            except apsw.CantOpenError,e:
                raise StopIteration

            gc.disable()
            for row in self.xexec:
                yield row
            gc.enable()

            self.part += 1

def Source():
    return vtbase.VTGenerator(UnionAllDB)


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
