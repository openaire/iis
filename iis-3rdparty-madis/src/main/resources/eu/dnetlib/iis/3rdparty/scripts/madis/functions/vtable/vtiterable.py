"""
Basis code for
OnetoN and NtoN operators
"""

import setpath
import functions
import apsw
from lib import argsparse

from lib import schemaUtils


# Decorator to extended a function by calling first another function with no arguments

def echocall(func):
    def wrapper(*args, **kw):
        if functions.settings['vtdebug']:
            obj=args[0]
            Extra=""
            if 'tablename' in obj.__dict__:
                Extra=obj.tablename
            print "Table %s:Before Calling %s.%s(%s)" %(Extra+str(obj),obj.__class__.__name__,func.__name__,','.join([repr(l) for l in args[1:]]+["%s=%s" %(k,repr(v)) for k,v in kw.items()]))
#            aftermsg="Table %s:After Calling %s.%s(%s)" %(Extra,obj.__class__.__name__,func.__name__,','.join([repr(l) for l in args[1:]]+["%s=%s" %(k,repr(v)) for k,v in kw.items()]))
        return func(*args, **kw)
    return wrapper


class SourceVT:
    def __init__(self,table,boolargs=None,nonstringargs=None,needsescape=None,staticschema=False,notsplit=None):
        self.tableObjs=dict()
        self.tableCl=table
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
        self.staticschema=staticschema

    @echocall
    def Create(self, db, modulename, dbname, tablename,*args):        
        dictargs={'tablename':tablename,'db':db,'dbname':dbname,'modulename':modulename}
        self.tableObjs[tablename]=LTable(self.tableCl,self.tableObjs,self.boolargs,self.nonstringargs,self.needsescape,self.notsplit,self.staticschema,*args,**dictargs)
        return [self.tableObjs[tablename].getschema(),self.tableObjs[tablename]]

    @echocall
    def Connect(self, db, modulename, dbname, tablename,*args):
        if tablename not in self.tableObjs:
            dictargs={'tablename':tablename,'db':db,'dbname':dbname,'modulename':modulename}
            self.tableObjs[tablename]=LTable(self.tableCl,self.tableObjs,self.boolargs,self.nonstringargs,self.needsescape,self.notsplit,self.staticschema,*args,**dictargs)
        return [self.tableObjs[tablename].getschema(),self.tableObjs[tablename]]


class emptyiter:
    def init(self):
        pass
    def __iter__(self):
        return self
    def next(self):
        raise StopIteration
    def close(self):
        pass


class LTable: ####Init means setschema and execstatus
    autostring='automatic_vtable'
    @echocall
    def __init__(self,vtable,tblist,boolargs,nonstringargs,needsescape,notsplit,staticschema,*args,**envars): # envars tablename, auto  , OPTIONAL []
        self.delayedexception=None
        self.tblist=tblist
        self.auto=False
        self.first=True
        self.staticschema=staticschema
        self.schema="create table %s('Error')" % (envars['tablename'])
        self.tablename=envars['tablename']
        self.envars=envars
        largs, kargs = [] ,dict()
        try:
            largs, kargs = argsparse.parse(args,boolargs,nonstringargs,needsescape,notsplit)
        except Exception,e:
            raise #functions.MadisError(e)
        if self.autostring in kargs:
            del kargs[self.autostring]
            self.auto=True
        ####init schema and set
        try:
            self.vtable=vtable(envars,largs,kargs)
            if not self.staticschema:
                self.iter=self.vtable.open()
            self._setschema()
            if not self.auto and not self.staticschema:
                if functions.settings['vtdebug']:
                    print "Manual vtable creation:Closing Vtable iterator"
                self.iter.close()
        except (StopIteration,apsw.ExecutionCompleteError),e: ###
            try:
                raise functions.DynamicSchemaWithEmptyResultError(self.envars['modulename'])
            finally:
                try:
                    self.iter.close()
                except:
                    pass
            
    @echocall
    def _setschema(self):
        descr=self.vtable.getdescription() ### get list of tuples columnname, type
        self.schema=schemaUtils.CreateStatement(descr, self.tablename)

    @echocall
    def getschema(self):
        if functions.settings['tracing']:
            print 'VT_Schema: %s' %(self.schema)
        return self.schema

    @echocall
    def BestIndex(self, *args):
        return (None, 0, None, False, 1000)

    @echocall
    def Open(self):
        if self.delayedexception:
            raise self.delayedexception[1], None, self.delayedexception[2] ### Re - raise Exception of Create
        if self.first and self.auto and not self.staticschema:
            self.first=False
            return Cursor(self,self.iter)
        else:
            try:
                itt=self.vtable.open()
            except StopIteration:
                itt=emptyiter()
            return Cursor(self,itt)
        
        return ret
    @echocall
    def reset(self,iter):
        iter.close()
        ret=self.vtable.open()
        return ret

    @echocall
    def Disconnect(self): 
        """
        This method is called when a reference to a virtual table is no longer used
        """
        if self.first and self.auto and not self.staticschema:
            self.iter.close()
            self.first=False
        if self.vtable.__class__.__dict__.has_key('disconnect'):
            self.vtable.disconnect()
    @echocall
    def Destroy(self):
        """
        This method is called when the table is no longer used
        """
        if self.first and self.auto and not self.staticschema:
            self.iter.close()
        del self.tblist[self.tablename]
        if self.vtable.__class__.__dict__.has_key('destroy'):
            self.vtable.destroy()



# Represents a cursor
class Cursor:
    __slots__ = ("Next", "Close", "Column", "Rowid", "Eof", "pos", "row", "table", "eof", "iterNext", "iter")

    @echocall
    def __init__(self, table,iter):
        self.table=table
        self.iter=iter
        self.iterNext = self.iter.next
        self.row=None
        self.tablename=table.tablename
        self.firsttime=True
        self.pos=0
        self.Column = lambda col: self.row[col]
        self.Rowid = lambda: self.pos + 1
        self.Eof = lambda: self.eof
        
    @echocall
    def Filter(self, *args):
        self.eof=False
        self.pos=-1

        if not self.firsttime:
            self.iter=self.table.reset(self.iter)
            self.iterNext = self.iter.next
        self.firsttime=False
        self.Next()

#    @echocall #-- Commented out for speed reasons
#     def Eof(self):
#         return self.eof

    #@echocall
    # def Rowid(self):
    #     return self.pos+1

#    @echocall #-- Commented out for speed reasons
    def ColumnStop(self, col):
        raise functions.OperatorError(self.table.envars['modulename'],"Not enough data in input")

#    @echocall #-- Commented out for speed reasons
#     def Column(self, col):
#         return self.row[col]

#    @echocall #-- Commented out for speed reasons
    def Next(self):
        try:
            self.row = self.iterNext()
#            self.pos+=1
        except StopIteration:
            self.row=None
            self.eof=True

    @echocall
    def Close(self):
        try:
            self.iter.close()
        except AttributeError:
            pass

def unify(slist):
    if len(set(slist))==len(slist):
        return slist
    eldict={}
    for s in slist:
        if s in eldict:
            eldict[s]+=1
        else:
            eldict[s]=1
    for val,fr in eldict.items():
        if fr==1:
            del eldict[val]
    for val in eldict:
        eldict[val]=1
    uniquelist=[]
    for s in slist:
        if s in eldict:
            uniquelist+=[s+str(eldict[s])]
            eldict[s]+=1
        else:
            uniquelist+=[s]

    return uniquelist

import re
onlyalphnum=re.compile('[a-zA-Z]\w*$')

def schemastr(tablename,colnames,typenames=None):
    stripedcolnames=[el if onlyalphnum.match(el) else '"'+el.replace('"','""')+'"' for el in colnames]
    if not typenames:
        return "create table %s(%s)" %(tablename,','.join(['"'+str(c)+'"' for c in unify(stripedcolnames)]))
    else:
        stripedtypenames=['' if el.lower()=="none" else el if onlyalphnum.match(el) else '"'+el.replace('"','""')+'"' for el in typenames]
        return "create table %s(%s)" %(tablename,','.join([str(c)+' '+str(t) for c,t in zip(unify(stripedcolnames),stripedtypenames)]))
