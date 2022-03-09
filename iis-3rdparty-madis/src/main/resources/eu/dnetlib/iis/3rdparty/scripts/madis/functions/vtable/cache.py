"""
.. function:: cache(query:None) -> [the same schema as input query]
    
Caches and indexes in memory the input query. Its output has exactly the same schema as
the input query.

Examples:

.. doctest::
    >>> table1('''
    ... 1   2
    ... 3   4
    ... 5   6
    ... 7   8
    ... 10  1
    ... ''')

    >>> sql("select * from (cache select * from table1) order by a desc,b asc")
    a  | b
    ------
    10 | 1
    7  | 8
    5  | 6
    3  | 4
    1  | 2

    >>> sql("select * from (cache select * from table1) where a=b")

    >>> table2('''
    ... 5   1
    ... 5   2
    ... 5   3
    ... 2   3
    ... 2   2
    ... 2   1
    ... ''')

    >>> sql("select * from (cache select * from table2) where b>=2 order by a desc,b desc")
    a | b
    -----
    5 | 3
    5 | 2
    2 | 3
    2 | 2

    >>> sql("select * from (cache select * from table2) where 3>=a and 3<=b")
    a | b
    -----
    2 | 3


.. seealso::

    * :ref:`tutcache`

"""

registered=True
from . import setpath
import functions
import apsw
import itertools
import operator
from lib import argsparse
from lib import kdtree
from lib import schemaUtils
import json

constraints={
2:'SQLITE_INDEX_CONSTRAINT_EQ',
32:'SQLITE_INDEX_CONSTRAINT_GE',
4:'SQLITE_INDEX_CONSTRAINT_GT',
8:'SQLITE_INDEX_CONSTRAINT_LE',
16:'SQLITE_INDEX_CONSTRAINT_LT',
64:'SQLITE_INDEX_CONSTRAINT_MATCH'
}


# Decorator to extended a function by calling first another function with no arguments

def echocall(func):
    def wrapper(*args, **kw):
        obj=args[0]
        Extra=""
        if 'tablename' in obj.__dict__:
            Extra=obj.tablename
        if functions.settings['vtdebug']:
            print("Table %s:Before Calling %s.%s(%s)" %(Extra+str(obj),obj.__class__.__name__,func.__name__,','.join([repr(l) for l in args[1:]]+["%s=%s" %(k,repr(v)) for k,v in list(kw.items())])))
            aftermsg="Table %s:After Calling %s.%s(%s)" %(Extra,obj.__class__.__name__,func.__name__,','.join([repr(l) for l in args[1:]]+["%s=%s" %(k,repr(v)) for k,v in list(kw.items())]))
        a=func(*args, **kw)
        if functions.settings['vtdebug']:
            pass
            #print aftermsg
        return a
        #return func(*args, **kw)
    return wrapper


class Source:
    def __init__(self,boolargs=[],nonstringargs=dict(),needsescape=[]):
        self.tableObjs=dict()
        self.boolargs=boolargs
        self.nonstringargs=nonstringargs
        self.needsescape=needsescape
    @echocall
    def Create(self, db, modulename, dbname, tablename,*args):
        
        dictargs={'tablename':tablename,'db':db,'dbname':dbname,'modulename':modulename}
        self.tableObjs[tablename]=LTable(self.tableObjs,self.boolargs,self.nonstringargs,self.needsescape,*args,**dictargs)
        return [self.tableObjs[tablename].getschema(),self.tableObjs[tablename]]
    @echocall
    def Connect(self, db, modulename, dbname, tablename,*args):
        if tablename not in self.tableObjs:
            return Create(self, db, modulename, dbname, tablename,*args)
        return [self.tableObjs[tablename].getschema(),self.tableObjs[tablename]]

class LTable: ####Init means setschema and execstatus
    autostring='automatic_vtable'
    @echocall
    def __init__(self,tblist,boolargs,nonstringargs,needsescape,*args,**envars): # envars tablename, auto  , OPTIONAL []
        self.delayedexception=None
        self.tblist=tblist
        self.auto=False
        self.first=True
        self.schema="create table %s('Error')" % (envars['tablename'])
        self.tablename=envars['tablename']
        self.description=None
        self.consdict={}
        self.coldata=[]
        self.rowids=[]
        self.kdindex=None
        self.lastcalculatedidx=None
        self.ordered=False

        largs, kargs = [] ,dict()
        try:
            largs, kargs = argsparse.parse(args,boolargs,nonstringargs,needsescape)
        except Exception as e:
            raise functions.MadisError(e)
        try:
            q=envars['db'].cursor().execute(kargs['query'])
            self.description=q.getdescription()
        except apsw.ExecutionCompleteError:
            raise functions.DynamicSchemaWithEmptyResultError(__name__.rsplit('.')[-1])

        self._setschema()

        self.data = []
        ro = []
        dedupl = {}
        for r in q:
            ro = []
            for i in r:
                if i not in dedupl:
                    dedupl[i] = i
                    ro.append(i)
                else:
                    ro.append(dedupl[i])

            self.data.append(tuple(ro))

        del(dedupl)


    @echocall
    def _setschema(self):
        descr=self.description ### get list of tuples columnname, type
        self.schema=schemaUtils.CreateStatement(descr, self.tablename)

    @echocall
    def getschema(self):
        if functions.settings['tracing']:
            print("VT schema:%s" %(self.schema))
        return self.schema

    @echocall
    def BestIndex(self, constraint_param, orderbys):
        indexes = []
        newcons = []
        i = 0

        for c in constraint_param:
            if c[1] == apsw.SQLITE_INDEX_CONSTRAINT_MATCH:
                indexes.append(None)
            else:
                indexes.append((i, True))
                i += 1
                newcons.append(c)

        consname = json.dumps(newcons, separators=(',', ':')) + json.dumps(orderbys, separators=(',', ':'))
        self.consdict[consname] = (newcons, orderbys)
        cost = 0

        # Cost of scan
        if newcons == []:
            cost = len(self.data)

        return indexes, 0, consname, True, cost

    @echocall
    def Open(self):
        return Cursor(self)

    @echocall
    def Disconnect(self):
        pass

    @echocall
    def Destroy(self):
        """
        This method is called when the table is no longer used
        """
        del(self.data)
        del(self.kdindex)
        del(self.tblist[self.tablename])

# Represents a cursor
class Cursor:
    def __init__(self, table):
        self.table=table
        self.eof=True
        self.pos=0
        self.row=[]
        
    # @echocall #-- Commented out for speed reasons
    def Filter(self, indexnum, indexname, constraintargs):
        self.eof=False
        constraints, orderbys = self.table.consdict[indexname]

        if self.table.lastcalculatedidx!=(constraints,orderbys):
            self.calculate_indexes(constraints,orderbys)
            self.table.lastcalculatedidx=(constraints,orderbys)
        
        if len(constraints)==0:
            self.resultrows=iter(self.table.data)
        else:
            self.resultrows=kdtree.query(self.table.kdindex, constraints, constraintargs)
            if self.table.ordered:
                self.resultrows=iter(sorted(list(self.resultrows),key=operator.itemgetter(self.table.orderindex) ))
        
        try:
            self.row=next(self.resultrows)
        except KeyboardInterrupt:
            raise
        except:
            self.eof=True

#    @echocall #-- Commented out for speed reasons
    def Eof(self):
        return self.eof

#    @echocall #-- Commented out for speed reasons
    def Rowid(self):
        return self.pos

#    @echocall #-- Commented out for speed reasons
    def Column(self, col):
        return self.row[col]

#    @echocall #-- Commented out for speed reasons
    def Next(self):
        try:
            self.row=next(self.resultrows)
        except KeyboardInterrupt:
            raise         
        except:
            self.eof=True
    
    @echocall
    def Close(self):
        del(self.resultrows)

    def calculate_indexes(self,cons,orderbys):
        self.table.ordered=False
        if len(orderbys)!=0 and len(self.table.data)!=0:
            self.table.ordered=True
            self.table.orderindex=len(self.table.data[0])
            for o in reversed(orderbys):
                self.table.data.sort(key=operator.itemgetter(o[0]),reverse=o[1])
            self.table.data=[x+(y,) for x,y in zip(self.table.data,itertools.count())]

        idxs=[x[0] for x in cons]
        self.table.kdindex=kdtree.kdtree(self.table.data, idxs)

import re
onlyalphnum=re.compile(r'[a-zA-Z]\w*$')

def schemastr(tablename,colnames,typenames=None):
    stripedcolnames=[el if onlyalphnum.match(el) else '"'+el.replace('"','""')+'"' for el in colnames]
    if not typenames:
        return "create table %s(%s)" %(tablename,','.join(['"'+str(c)+'"' for c in unify(stripedcolnames)]))
    else:
        stripedtypenames=['' if el=="None" else el if onlyalphnum.match(el) else '"'+el.replace('"','""')+'"' for el in typenames]
        return "create table %s(%s)" %(tablename,','.join([str(c)+' '+str(t) for c,t in zip(unify(stripedcolnames),stripedtypenames)]))

def unify(slist):
    if len(set(slist))==len(slist):
        return slist
    eldict={}
    for s in slist:
        if s in eldict:
            eldict[s]+=1
        else:
            eldict[s]=1
    for val,fr in list(eldict.items()):
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


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    from . import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()