"""
Basis code for Virtual table. The schema is extracted from firstrow.
"""

from . import setpath
import functions
import apsw
from lib import argsparse ,schemaUtils
import sys
if hasattr(sys, 'pypy_version_info'):
    PYPY = True
    from __pypy__ import newlist_hint
else:
    PYPY = False
    newlist_hint = lambda size: []

autostring='automatic_vtable:1'

# Decorator to extended a function by calling first another function with no arguments
def echocall(func):
    def wrapper(*args, **kw):
        if functions.settings['vtdebug']:
            obj=args[0]
            Extra=""
            if 'tablename' in obj.__dict__:
                Extra=obj.tablename
            print("Table %s:Before Calling %s.%s(%s)" %(Extra+str(obj),obj.__class__.__name__,func.__name__,','.join([repr(l) for l in args[1:]]+["%s=%s" %(k,repr(v)) for k,v in list(kw.items())])))
        return func(*args, **kw)
    return wrapper

class VT(object):
    def parse(self,*args):
        return args
    def full_parse(self,args,boolargs=None,nonstringargs=None,needsescape=None, notsplit=None):
        return argsparse.parse(args,boolargs,nonstringargs,needsescape,notsplit)
    def open(self,*args,**kargs):
        raise NotImplementedError
    def disconnect(self):
        pass
    def destroy(self):
        pass

class VTGenerator(object):
    def __init__(self,fobj): ####parse auto argument and throw!!!!
        self.tableObjs=dict()
        self.fobj=fobj

    @echocall
    def Create(self, db, modulename, dbname, tablename,*args):
        envars={'tablename':tablename,'db':db,'dbname':dbname,'modulename':modulename}
        uargs=[argsparse.unquote(a) for a in args]
        if len(uargs)>0 and uargs[-1]==autostring:
            auto=True
            uargs=uargs[:-1]
        TableVT=self.fobj()
        parsedArgs = list(TableVT.parse(*uargs))
        iterFunc = lambda:TableVT.VTiter(*parsedArgs,**envars)
        openedIter = iterFunc()
        
        try:
            schema = next(openedIter)
        except (StopIteration,apsw.ExecutionCompleteError) as e:
            try:
                if hasattr(openedIter,'close'):
                    openedIter.close()
                    openedIter = None
            except:
                pass
            raise functions.DynamicSchemaWithEmptyResultError(envars['modulename'])
        except apsw.AbortError:
            openedIter.close()
            openedIter = iterFunc()
            schema = next(openedIter)

        self.tableObjs[tablename]=(schemaUtils.CreateStatement(schema,tablename),LTable(self.tableObjs, envars, TableVT, iterFunc, openedIter))
        if functions.settings['tracing']:
            print('VT_Schema: %s' %(self.tableObjs[tablename][0]))
        return self.tableObjs[tablename]
    @echocall
    def Connect(self, db, modulename, dbname, tablename,*args):
        if tablename not in self.tableObjs:
            return self.Create( db, modulename, dbname, tablename,*args)
        return self.tableObjs[tablename]

class LTable(object): ####Init means setschema and execstatus
    @echocall
    def __init__(self, tblist, envars, tableObj , iterFunc ,openedIter=None): # tablename, auto , cursorfunc, ommit tuple OPTIONAL []
        self.tblist=tblist
        self.envars=envars
        self.iterFunc=iterFunc
        self.openedIter=openedIter
        self.tableObj=tableObj
        self.cursors = []

    @echocall
    def BestIndex(self, *args):
        try:
            return self.tableObj.BestIndex(*args)
        except AttributeError:
            if functions.SQLITEAFTER380:
                return (None, 0, None, False, 1e99)
            else:
                return (None, 0, None, False, 1000)

    @echocall
    def Open(self):
        tmpIter = None
        if self.openedIter == None:
            tmpIter = self.iterFunc()
            next(tmpIter)
        else:
            tmpIter = self.openedIter
            self.openedIter = None

        c = Cursor(self.iterFunc, tmpIter, self.envars)
        self.cursors.append(c)
        return c

    def CloseCursors(self):
        for c in self.cursors:
            c.Close()
        self.cursors = []

    @echocall
    def Disconnect(self):
        """
        This method is called when a reference to a virtual table is no longer used
        """
        self.CloseCursors()
        if 'disconnect' in self.tableObj.__class__.__dict__:
            self.tableObj.disconnect()

    @echocall
    def Destroy(self):
        """
        This method is called when the table is no longer used
        """
        self.CloseCursors()
        del self.tblist[self.envars['tablename']]
        if 'destroy' in self.tableObj.__class__.__dict__:
            self.tableObj.destroy()

# Represents a cursor
class Cursor(object): ##### Needs Cursor Function , Iterator instance, tablename ...... if has close
    __slots__ = ("Column", "Next", "Eof", "Rowid", "iterNext", "pos", "row", "eof", "envars", "firsttime", "openIter", "iterFunc")

    @echocall
    def __init__(self, iterFunc, openIter, envars):
        self.envars = envars
        self.firsttime = True
        self.openIter=openIter
        self.iterNext = self.openIter.__next__
        self.iterFunc=iterFunc
        self.row = []
        self.eof = False
        self.pos = 0
        self.Column = lambda col: self.row[col]
        self.Rowid = lambda: self.pos + 1
        self.Eof = lambda: self.eof
        if PYPY:
            self.Next = self.NextPyPy
        else:
            self.Next = self.NextCPython

    # @echocall
    def Filter(self, *args):
        self.eof=False
        self.pos=0

        if not self.firsttime:
            if hasattr(self.openIter,'close'):
                self.openIter.close()
            self.openIter=self.iterFunc()
            self.iterNext = self.openIter.__next__
            self.iterNext()

        self.firsttime=False
        self.Next()
        return
    
    #@echocall #-- Commented out for speed reasons
    # def Eof(self):
    #     return self.eof

    #@echocall #-- Commented out for speed reasons
    # def Rowid(self):
    #     return self.pos

    #@echocall #-- Commented out for speed reasons
    # def Column(self, col):
    #     return self.row[col]

    #@echocall #-- Commented out for speed reasons
    def NextCPython(self):
        try:
            self.row = self.iterNext()
            self.Column = self.row.__getitem__
#            self.pos+=1
        except StopIteration:
            self.row = []
            self.eof = True
            self.Close()
        except Exception as e:
            if str(e) == 'generator raised StopIteration':
                self.row = []
                self.eof = True
                self.Close()

    def NextPyPy(self):
        try:
            self.row = self.iterNext()
#            self.pos+=1
        except StopIteration:
            self.row = []
            self.eof = True
            self.Close()

    # @echocall
    def Close(self):
        if hasattr(self.openIter, 'close'):
            self.openIter.close()
        self.openIter = None

