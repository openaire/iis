import os.path
import sys
import functions
import os
from itertools import izip
import cPickle
import cStringIO
import vtbase
import functions
import struct
import vtbase
import functions
import os
import gc
import re
import zlib
### Classic stream iterator
registered=True
BLOCK_SIZE = 200000000
import apsw
import time
import listser
class RC2DB(vtbase.VT):


    def VTiter(self, *args,**formatArgs):
        time1 = time.time()
        largs, dictargs = self.full_parse(args)
        where = None
        mode = 'row'
        input = cStringIO.StringIO()

        if 'file' in dictargs:
            where=dictargs['file']
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")
        col = 0

        filename, ext=os.path.splitext(os.path.basename(where))
        if 'cols' in dictargs:
            a = re.split(' |,| , |, | ,' , dictargs['cols'])
            column = [x for x in a if x != '']
        else:
            col = 1
        start = 0
        end = sys.maxint-1
        if 'start' in dictargs:
            start = int(dictargs['start'])
        if 'end' in dictargs:
            end = int(dictargs['end'])

        fullpath = str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(where)))))
        fileIterlist = []
        for x in xrange(start,end+1):
            try:
                fileIterlist.append(open(fullpath+"."+str(x), "rb"))
            except:
                break

        if fileIterlist == []:
            try:
                fileIterlist = [open(where, "rb")]
            except :
                raise  functions.OperatorError(__name__.rsplit('.')[-1],"No such file")
        cursor = []
        for filenum,fileObject in enumerate(fileIterlist):
            b = struct.unpack('!B',fileObject.read(1))
            schema = cPickle.load(fileObject)
            colnum = len(schema)
            readtype = '!'+'i'*colnum
            readsize = 4 * colnum

            if filenum == 0:
                yield schema

                def createdb(where, tname, schema, page_size=16384):
                            c=apsw.Connection(where)
                            cursor=c.cursor()
                            list(cursor.execute('pragma page_size='+str(page_size)+';pragma cache_size=-1000;pragma legacy_file_format=false;pragma synchronous=0;pragma journal_mode=OFF;PRAGMA locking_mode = EXCLUSIVE'))
                            create_schema='create table '+tname+' ('
                            create_schema+='`'+unicode(schema[0][0])+'`'+ (' '+unicode(schema[0][1]) if schema[0][1]!=None else '')
                            for colname, coltype in schema[1:]:
                                create_schema+=',`'+unicode(colname)+'`'+ (' '+unicode(coltype) if coltype!=None else '')
                            create_schema+='); begin exclusive;'
                            list(cursor.execute(create_schema))
                            insertquery="insert into "+tname+' values('+','.join(['?']*len(schema))+')'
                            return c, cursor, insertquery
                c, cursor, insertquery=createdb(where+".db", filename, schema)

            while True:
                try:
                    b = struct.unpack('!B',fileObject.read(1))
                except :
                    break
                if b[0]:
                    input.truncate(0)
                    ind = struct.unpack(readtype,fileObject.read(readsize))
                    input.write(fileObject.read(sum(ind)))
                    input.seek(0)
                    gc.disable()
                    cursor.executemany(insertquery, izip(*tuple(listser.loads(zlib.decompress(input.read(ind[col]))) for col in xrange(colnum))))
                    gc.enable()
                elif not b[0]:
                    schema = cPickle.load(fileObject)
        list(cursor.execute('commit'))
        c.close()

        try:
            for fileObject in fileIterlist:
                fileObject.close()
        except NameError:
            pass
        time2 = time.time()
        stats = open('compressionstatistics.tsv', 'a')
        stat = where
        statstr = stat + "\t" + str(time2-time1) + "\n"
        stats.write(statstr)

def Source():
    return vtbase.VTGenerator(RC2DB)

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



