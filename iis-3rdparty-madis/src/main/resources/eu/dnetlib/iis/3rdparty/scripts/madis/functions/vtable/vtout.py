"""
TODO : add logging on continue
---------------------------------
Base module for NtoOne operators:

Creates a virtual table with one columname : return_value and returns 1 if the execution succeds
                    and raises an exception or return 0 on failure

To create an NtoOneOperator as VT

def Source:
    return SourceNtoOne(PARAMETERS)

PARAMETERS:
1. Function to be called on execution (at the end there is the inteface of the function)
Optional parameters:
boolargs                : List of names of dictionary boolean parameters of input function
nonstringargs           : Dictionary of the translation of non-string and boolean parameters
connectionhandler       : If True the connection is passed to the running function
retalways               : If True any exeption during execution will be caught and 0 will be returned

Gets a function to execute on execution
Input Function INTERFACE:
1. Tuple of (Iterator on query result,tuple of columntypes)
2. OPTIONAL db connection (to receive this argument...)
3+ rest of parsed arguments in list and dictionary

IF function returns normally 1 value is returned, else exeption is raised EXCEPT if ....

"""

from . import setpath
from lib import argsparse
import functions
import logging
import itertools
import apsw

class doall(object):
    def __init__(self,query,connection,func,returnalways,passconnection,*args,**kargs):
        self.connection=connection
        self.query=query
        self.args=args
        self.func=func
        self.kargs=kargs
        self.returnalways=returnalways
        self.passconnection=passconnection

    def run(self):        
        c = self.connection.cursor()
        try:
            cexec = c.execute(self.query, parse=False)

            try:
                schema = c.getdescriptionsafe()
            except functions.ExecutionCompleteError:
                raise functions.DynamicSchemaWithEmptyResultError("got empty input")
        
            if self.passconnection:
                try:
                    self.func(cexec, schema, self.connection, *self.args, **self.kargs)
                except apsw.AbortError:
                    cexec = c.execute(self.query, parse=False)
                    self.func(cexec, schema, self.connection, *self.args, **self.kargs)
            else:
                try:
                    self.func(cexec, schema, *self.args, **self.kargs)
                except apsw.AbortError:
                    cexec = c.execute(self.query, parse=False)
                    self.func(cexec, schema, *self.args, **self.kargs)

            ret = True
        except Exception as e:
            if functions.settings['logging']:
                lg = logging.LoggerAdapter(logging.getLogger(__name__), {"flowname": functions.variables.flowname})
                lg.exception(e)
            if self.returnalways:
                return False
            else:
                if functions.settings['tracing']:
                    import traceback
                    print("---Deep Execution traceback--")
                    print(traceback.print_exc())
                raise functions.MadisError(e)
        finally:            
            try:
                c.close()
            except:
                pass
        return ret

class SourceNtoOne:
    def __init__(self, func, boolargs=None, nonstringargs=None, needsescape=None, notsplit=None, connectionhandler=False, retalways=False):
        self.func=func
        if boolargs==None:
            self.boolargs=[]
        else:
            self.boolargs=boolargs
        if nonstringargs==None:
            self.nonstringargs=dict()
        else:
            self.nonstringargs=nonstringargs
        if needsescape==None:
            self.needsescape=[]
        else:
            self.needsescape=needsescape
        if notsplit==None:
            self.notsplit=[]
        else:
            self.notsplit=notsplit
        self.connectionhandler=connectionhandler
        self.retalways=retalways
    def Create(self, db, modulename, dbname, tablename, *args):

        schema="create table %s(return_value)" %(tablename)
        return [schema,Table(lambda:maincode(args,self.boolargs,self.nonstringargs,self.needsescape,self.notsplit,db,self.func,self.retalways,self.connectionhandler))]
    Connect=Create

def maincode(args,boolargs,nonstringargs,needsescape,notsplit,db,func,retalways,connectionhandler):
    autostring='automatic_vtable'
    try:
        largs, kargs = argsparse.parse(args,boolargs,nonstringargs,needsescape,notsplit)
    except Exception as e:
        raise functions.MadisError(e)
    if 'query' not in kargs:
        raise functions.OperatorError(func.__globals__['__name__'].rsplit('.')[-1],"needs query argument ")
    query=kargs['query']
    del kargs['query']
    if autostring in kargs:
        del kargs[autostring]
    return doall(query,db,func,retalways,connectionhandler,*largs,**kargs)

# Represents a table
class Table:
    def __init__(self, dobj):
        self.dobj=dobj

    def BestIndex(self, *args):
        return (None, 0, None, False, 1000)

    def Rollback(self,*args):
        pass

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    def Destroy(self):
        pass

# Represents a cursor
class Cursor:
    def __init__(self, table):
        self.table=table
        self.row=None

    def Filter(self, *args):        
        if self.table.dobj().run():
            self.row=[1]
        else:
            self.row=[0] 
        self.eof=False
        self.pos=0

    def Eof(self):        
        return self.eof

    def Rowid(self):        
        return self.pos+1

    def Column(self, col):        

        return self.row[col]

    def Next(self):  
        self.eof = True

    def Close(self):
        pass