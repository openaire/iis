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

class UnionAllRC(vtbase.VT):


    def VTiter(self, *args,**formatArgs):
        largs, dictargs = self.full_parse(args)
        where = None
        mode = 'row'
        input = cStringIO.StringIO()

        if 'file' in dictargs:
            where=dictargs['file']
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")
        col = 0

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

        for filenum,fileObject in enumerate(fileIterlist):
            b = struct.unpack('!B',fileObject.read(1))
            schema = cPickle.load(fileObject)
            colnum = len(schema)
            readtype = '!'+'i'*colnum
            readsize = 4 * colnum
            if filenum == 0:
                yield schema

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
                    for row in izip(*tuple(cPickle.loads(zlib.decompress(input.read(ind[col]))) for col in xrange(colnum))):
                        yield row
                elif not b[0]:
                    schema = cPickle.load(fileObject)
                    
        try:
            for fileObject in fileIterlist:
                fileObject.close()
        except NameError:
            pass


def Source():
    return vtbase.VTGenerator(UnionAllRC)

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


