# To change this template, choose Tools | Templates
# and open the template in the editor.
import os.path
import setpath
import sys
import imp
from vtout import SourceNtoOne
from lib.dsv import writer
import gzip
from lib.ziputils import ZipIter
import functions
from lib.vtoutgtable import vtoutpugtformat
import lib.inoutparsing
import os
import apsw
from collections import defaultdict
import marshal
from itertools import izip
import itertools
import cPickle as cPickle
import struct
import gc
import cStringIO as cStringIO
import marshal
import zlib
from collections import OrderedDict
from array import array
from itertools import chain
import zlib



BLOCK_SIZE = 65536000

registered=True

def getSize(v):
    t = type(v)

    if t == unicode:
        return 52 + 4*len(v)

    if t in (int, float, None):
        return 24

    return 37 + len(v)

def outputData(diter, schema, connection, *args, **formatArgs):
    ### Parameter handling ###

    where=None
    mode = 'row'
    if len(args)>0:
        where=args[0]
    elif 'file' in formatArgs:
        where=formatArgs['file']
    else:
        raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")
    if 'file' in formatArgs:
        del formatArgs['file']
    if 'mode' in formatArgs:
        mode = formatArgs['mode']
    filename, ext=os.path.splitext(os.path.basename(where))
    fullpath=os.path.split(where)[0]
    if 'split' not in formatArgs:
        fileIter=open(where, "w+b")
        fastPickler = cPickle.Pickler(fileIter, 1)
        fastPickler.fast = 1
    else:
        fileIter = 1


    if mode == 'simplecol':
        pass


   


    if mode == 'spaconedict':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = set([])
        start = 0
        step = 65535 / colnum
        bsize = 0
        numrows = 0
        dictlimit = step - colnum
        current = [[] for _ in xrange(colnum)]
        prevdicts = {}
        count = 0
        while True:
            maxlen = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            if len(nditer) != 0:
                count += len(nditer[0])
            for i,col in enumerate(nditer):
                current[i] += col
                setcol.update(col)
                l = len(setcol)
                if l > maxlen:
                    maxlen = l
            step = (65535 - maxlen) / colnum
            if step < 5000 or len(nditer) == 0:
                prev = fileIter.tell()
                headindex = [0 for _ in xrange(colnum+2)]
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                headindex[i] = fileIter.tell()
                s = sorted(setcol)
                finallist =[]
                for val in s:
                    if val not in prevdicts:
                        finallist.append(val)
                prevdicts = dict(((x,y) for y,x in enumerate(s)))
                marshal.dump(list(finallist),fileIter,2)
                cur = zip(*current)
                for i in xrange(count):
                    array('H',[prevdicts[y] for y in cur[i]]).tofile(fileIter)
                headindex[i+1] = fileIter.tell()
                headindex[i+2] = count
                count=0
                fileIter.seek(prev)
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.seek(headindex[colnum])
                current = [[] for _ in xrange(colnum)]
                setcol = set([])
                gc.collect()
                step = 65535 / colnum
            if len(nditer)==0:
                break



    if mode == 'sortedspac':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        listofarrays = []
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [set([]) for _ in xrange(colnum)]
        count = 0
        blocknum = 0
        bsize = 0
        while True:
            maxlen = 0
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                for c in z:
                    bsize+=sys.getsizeof(c)
                listofnditers.append(z)

                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

            step = dictsize - maxlen
            if step < stopstep or ret or bsize>300000000:
                bsize=0
                prev = fileIter.tell() + 4*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]

                if blocknum == 0:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                            s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
                            #output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                            listofarrays.append(s)
                        else:
                            prevsets[i] = set(setcol[i]).copy()
                            s = sorted(setcol[i])
                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            if len(s)<256:
                                indextype='B'
                            else:
                                indextype='H'
                            listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            #output.write(compressorlistcols[i].compress(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl] ).tostring()))
                else:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
                            #output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                            listofarrays.append(s)
                        else:
                            difnew = list(setcol[i]-prevsets[i])
                            difold = list(prevsets[i]-setcol[i])
                            prevsets[i].intersection_update(setcol[i])
                            prevsets[i].update(difnew)
                            prevsets[i].update(difold[:(dictsize-len(prevsets[i]))])
                            towrite = {}
                            le = len(difold)
                            si = len(coldicts[i])
                            for l,j in enumerate(difnew):
                                if l<le:
                                    towrite[j] = coldicts[i][difold[l]]
                                else:
                                    towrite[j] = si
                                    si+=1
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(marshal.dumps(towrite.keys(),2)))
                                output.write(zlib.compress(array(indextype,towrite.values()).tostring()))
                            listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                        #output.write(compressorlistcols[i].compress(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl] ).tostring()))
#
                z = zip(*listofarrays)
                ##edw elegxw prwta oti uparxei kapoio paxcol
                z.sort(key=lambda z: z[paxcols[0]])
                li = zip(*z)


#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(marshal.dumps(l)))

                for x in li:
                       output.write(zlib.compress(marshal.dumps(sorted(x),2)))


                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count

                count=0
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                listofarrays = []
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1

            if ret:
                break



    if mode == 'sspac':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        #listofarrays = []
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [set([]) for _ in xrange(colnum)]
        count = 0
        blocknum = 0
        bsize = 0

        while True:
            maxlen = 0
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                for c in z:
                    bsize+=sys.getsizeof(c)
                listofnditers.append(z)

                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

            step = dictsize - maxlen
            if step < stopstep or ret or bsize>300000000:
                bsize=0
                prev = fileIter.tell() + 4*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]
                listofvals = []
                for j in xrange(colnum):
                     listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])
                listofvals1 = zip(*listofvals)
                listofvals1.sort(key=lambda listofvals1: listofvals1[10])
                listofvals = zip(*listofvals1)
                for s in setcol:
                    s.clear()
                for l in xrange(colnum):
                    setcol[l] = set(listofvals[l])

                print listofvals[10]
                if blocknum == 0:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                            #s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        else:
                            prevsets[i] = set(setcol[i]).copy()
                            s = sorted(setcol[i])
                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            if len(s)<256:
                                indextype='B'
                            else:
                                indextype='H'
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(marshal.dumps(s,2)))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i] ]).tostring()))
                else:

                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            #s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        else:
                            difnew = list(setcol[i]-prevsets[i])
                            difold = list(prevsets[i]-setcol[i])
                            prevsets[i].intersection_update(setcol[i])
                            prevsets[i].update(difnew)
                            prevsets[i].update(difold[:(dictsize-len(prevsets[i]))])
                            towrite = {}
                            le = len(difold)
                            si = len(coldicts[i])
                            for l,j in enumerate(difnew):
                                if l<le:
                                    towrite[j] = coldicts[i][difold[l]]
                                else:
                                    towrite[j] = si
                                    si+=1
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(marshal.dumps(towrite.keys(),2)))
                                output.write(zlib.compress(array(indextype,towrite.values()).tostring()))
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))

#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(l.tostring()))



                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count

                count=0
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                #listofarrays = []
                listofnditers = []
                listofvals=[]
                gc.collect()
                step = dictsize
                blocknum+=1

            if ret:
                break
            
    if mode == 'sortspac':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        #listofarrays = []
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [set([]) for _ in xrange(colnum)]
        count = 0
        blocknum = 0
        bsize = 0
        while True:
            maxlen = 0
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                for c in z:
                    bsize+=sys.getsizeof(c)
                listofnditers.append(z)

                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

            step = dictsize - maxlen
           



            if step < stopstep or ret or bsize>300000000:
                bsize=0
                prev = fileIter.tell() + 4*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]

                listofvals = []
                for j in xrange(colnum):
                    listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])
                listofvals1 = zip(*listofvals)
                listofvals1.sort(key=lambda listofvals1: listofvals1[5])
                listofvals = zip(*listofvals1)
                for s in setcol:
                    s.clear()
                for l in xrange(colnum):
                    setcol[l] = set(listofvals[l])


                if blocknum == 0:

                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                            print i
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        else:
                            prevsets[i] = set(setcol[i]).copy()
                            s = sorted(setcol[i])
                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            if len(s)<256:
                                indextype='B'
                            else:
                                indextype='H'
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(marshal.dumps(s,2)))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            #l = [coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]
                            #z = [coldict[val] for val in listofvals[i]]
                            #print l
                            #print i
                            #print len(z)

                else:
#                    listofnditers1 = zip(*listofnditers)
#                    listofnditers1.sort(key=lambda listofnditers1: listofnditers1[paxcols[0]])
#                    listofnditers = zip(*listofnditers1)
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        else:
                            difnew = list(setcol[i]-prevsets[i])
                            difold = list(prevsets[i]-setcol[i])
                            prevsets[i].intersection_update(setcol[i])
                            prevsets[i].update(difnew)
                            prevsets[i].update(difold[:(dictsize-len(prevsets[i]))])
                            towrite = {}
                            le = len(difold)
                            si = len(coldicts[i])
                            for l,j in enumerate(difnew):
                                if l<le:
                                    towrite[j] = coldicts[i][difold[l]]
                                else:
                                    towrite[j] = si
                                    si+=1
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(marshal.dumps(towrite.keys(),2)))
                                output.write(zlib.compress(array(indextype,towrite.values()).tostring()))
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))

#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(l.tostring()))



                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count

                count=0
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                #listofarrays = []
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1

            if ret:
                break



    if mode == 'corelspac':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        #listofarrays = []
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [set([]) for _ in xrange(colnum)]
        corelset = {}
        correlatedcols = {}
        correlated = set()
        firstgroup = set()
        count = 0
        blocknum = 0
        bsize = 0
        while True:
            maxlen = 0
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                for c in z:
                    bsize+=sys.getsizeof(c)
                listofnditers.append(z)

                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

            step = dictsize - maxlen
            if step < stopstep or ret or bsize>300000000:
                bsize=0
                prev = fileIter.tell() + 4*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]
                listofvals = []
                for j in xrange(colnum):
                    listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])

                if blocknum == 0:


                    sortdict = {}
                    for i in xrange(colnum):
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                        sortdict[i] = len(setcol[i])
                    le = sorted(sortdict, key=sortdict.get)
                    ginomeno = 1

                    for i in le:
                        ginomeno = ginomeno * sortdict[i]
                        if ginomeno > 256:
                            break
                        else:
                            pass #firstgroup.add(i)
                    print firstgroup
                    num = colnum-len(paxcols)-len(firstgroup)
                    if num%2 != 0 and len(firstgroup)!=0:
                        firstgroup.remove(le[0])
                    print firstgroup
                    print paxcols
                    for i in xrange(colnum-1):
                        for j in xrange(i+1,colnum):
                            if  i not in correlated and j not in correlated and i not in firstgroup and j not in firstgroup:
                                l = zip(listofvals[i],listofvals[j])
                                #print (float(len(set(l)))/len(set(listofvals[i]))+len(set(listofvals[j]))) , i , j
                                if len(set(l)) < 10000:
                                    correlatedcols[i] = j
                                    correlated.add(i)
                                    correlated.add(j)
                                    i+=1
                    print correlatedcols
                    check = 0
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if i in paxcols:
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        elif i in firstgroup and check == 0:

                            check=1
                        elif i in correlatedcols:
                                corellated = zip(listofvals[i],listofvals[correlatedcols[i]])
                                corelset[i] = set(corellated)
                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset[i])))
                                coldict = coldicts[i]
                                s = zip(*corelset[i])
                                output.write(zlib.compress(marshal.dumps(s[0],2)))
                                output.write(zlib.compress(marshal.dumps(s[1],2)))
                                if len(corelset[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset[i]] ).tostring()))

                        elif i in correlated or (i in firstgroup and check==1):
                                    pass
                        else:
                            prevsets[i] = set(setcol[i]).copy()
                            s = sorted(setcol[i])
                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            if len(s)<256:
                                indextype='B'
                            else:
                                indextype='H'
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(marshal.dumps(s,2)))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                else:
#                    listofnditers1 = zip(*listofnditers)
#                    listofnditers1.sort(key=lambda listofnditers1: listofnditers1[paxcols[0]])
#                    listofnditers = zip(*listofnditers1)
                    check=0
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            output.write(zlib.compress(marshal.dumps(listofvals[i],2)))
                            #listofarrays.append(s)
                        elif i in firstgroup and check == 0:
                            check=1
                        elif i in correlatedcols:
                                corellated = zip(listofvals[i],listofvals[correlatedcols[i]])
                                corelset1 = set(corellated)
                                difnew = list(corelset1-corelset[i])
                                difold = list(corelset[i]-corelset1)
                                corelset[i].intersection_update(corelset1)
                                corelset[i].update(difnew)
                                corelset[i].update(difold)
                                towrite = {}
                                le = len(difold)
                                si = len(coldicts[i])
                                for l,j in enumerate(difnew):
                                    if l<le:
                                        towrite[j] = coldicts[i][difold[l]]
                                    else:
                                        towrite[j] = si
                                        si+=1

                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset[i])))
                                coldict = coldicts[i]
                                s = zip(*towrite.keys())
                                if len(s)>0:
                                    output.write(zlib.compress(marshal.dumps(s[0],2)))
                                    output.write(zlib.compress(marshal.dumps(s[1],2)))
                                if len(corelset[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset[i]] ).tostring()))
                        elif i in correlatedcols.values() or (i in firstgroup and check==1):
                                    pass
                        else:
                            difnew = list(setcol[i]-prevsets[i])
                            difold = list(prevsets[i]-setcol[i])
                            prevsets[i].intersection_update(setcol[i])
                            prevsets[i].update(difnew)
                            prevsets[i].update(difold[:(dictsize-len(prevsets[i]))])
                            towrite = {}
                            le = len(difold)
                            si = len(coldicts[i])
                            for l,j in enumerate(difnew):
                                if l<le:
                                    towrite[j] = coldicts[i][difold[l]]
                                else:
                                    towrite[j] = si
                                    si+=1
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(marshal.dumps(towrite.keys(),2)))
                                output.write(zlib.compress(array(indextype,towrite.values()).tostring()))
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))

#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(l.tostring()))



                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count

                count=0
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                #listofarrays = []
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1

            if ret:
                break



    if mode == 'corelspacfinal':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        firstgroup = set()
        correlatedcols = {}
        correlated = set()
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [[] for _ in xrange(colnum)]
        corelset = {}
        count = 0
        blocknum = 0
        bsize = 0
        while True:
            maxlen = 0
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                for c in z:
                    bsize+=sys.getsizeof(c)
                listofnditers.append(z)

                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

            step = dictsize - maxlen
            if step < stopstep or ret or bsize>300000000:
                bsize=0
                prev = fileIter.tell() + 8*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]
                listofvals = []
                for j in xrange(colnum):
                    listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])

                if blocknum == 0:
                    sortdict = {}
                    for i in xrange(colnum):
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                        sortdict[i] = len(setcol[i])
                    le = sorted(sortdict, key=sortdict.get)
                    ginomeno = 1

                    print paxcols
                    for i in xrange(colnum-1):
                        for j in xrange(i+1,colnum):
                            if  i not in correlated and j not in correlated :
                                l = zip(listofvals[i],listofvals[j])
                                #print (float(len(set(l)))/len(set(listofvals[i]))+len(set(listofvals[j]))) , i , j
                                if len(set(l)) < 10000:
                                    correlatedcols[i] = j
                                    correlated.add(i)
                                    correlated.add(j)
                                    i+=1
                    print correlatedcols

                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev

#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if (len(setcol[i])>55000):
                            paxcols.append(i)
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            sortedlist = sorted(listofvals[i])
                            output.write(zlib.compress(marshal.dumps(sortedlist,2)))
                            indextype = 'H'
                            indexeslist = []
                            for value in sortedlist:
                                indexeslist.append(listofvals[i].index(value))
                            output.write(zlib.compress(array(indextype,indexeslist).tostring()))
                            l[0] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])

                        elif i in correlatedcols:
                                corellated = zip(listofvals[i],listofvals[correlatedcols[i]])
                                corelset[i] = set(corellated)
                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset[i])))
                                coldict = coldicts[i]
                                s = zip(*corelset[i])
                                output.write(zlib.compress(marshal.dumps(s[0],2)))
                                output.write(zlib.compress(marshal.dumps(s[1],2)))
                                if len(corelset[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset[i]] ).tostring()))

                        elif i in correlated:
                                    pass
                            #listofarrays.append(s)
#                        elif i==8 or i==9:
#                            if i==8:
#                                pass
#                            if i==9:
#                                corellated = zip(listofvals[8],listofvals[9])
#                                corelset = set(corellated)
#                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset)))
#                                coldict = coldicts[i]
#                                s = zip(*corelset)
#                                output.write(zlib.compress(marshal.dumps(s[0],2)))
#                                output.write(zlib.compress(marshal.dumps(s[1],2)))
#                                indextype='B'
#                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset] ).tostring()))
                        else:
                            prevsets[i] = list(set(setcol[i]).copy())
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i])<256:
                                indextype='B'
                            else:
                                indextype='H'
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(zlib.compress(marshal.dumps(prevsets[i],2)))
                            l[0] = output.tell()
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[1] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[1])
                            print l
                else:
#                    listofnditers1 = zip(*listofnditers)
#                    listofnditers1.sort(key=lambda listofnditers1: listofnditers1[paxcols[0]])
#                    listofnditers = zip(*listofnditers1)
                    check = 0
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            sortedlist = sorted(listofvals[i])
                            output.write(zlib.compress(marshal.dumps(sortedlist,2)))
                            indextype = 'H'
                            indexeslist = []
                            for value in sortedlist:
                                indexeslist.append(listofvals[i].index(value))
                            output.write(zlib.compress(array(indextype,indexeslist).tostring()))
                            l[0] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])
                            #listofarrays.append(s)
#                        elif i==8 or i==9:
#                            if i==8:
#                                pass
#                            if i==9:
#                                corellated = zip(listofvals[8],listofvals[9])
#                                corelset1 = set(corellated)
#                                difnew = list(corelset1-corelset)
#                                difold = list(corelset-corelset1)
#                                corelset.intersection_update(corelset1)
#                                corelset.update(difnew)
#                                corelset.update(difold)
#                                towrite = {}
#                                le = len(difold)
#                                si = len(coldicts[i])
#                                for l,j in enumerate(difnew):
#                                    if l<le:
#                                        towrite[j] = coldicts[i][difold[l]]
#                                    else:
#                                        towrite[j] = si
#                                        si+=1
#
#                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset)))
#                                coldict = coldicts[i]
#                                s = zip(*towrite.keys())
#                                if len(s)>0:
#                                    output.write(zlib.compress(marshal.dumps(s[0],2)))
#                                    output.write(zlib.compress(marshal.dumps(s[1],2)))
#                                indextype='B'
#                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset] ).tostring()))
                        elif i in correlatedcols:
                                corellated = zip(listofvals[i],listofvals[correlatedcols[i]])
                                corelset1 = set(corellated)
                                difnew = list(corelset1-corelset[i])
                                difold = list(corelset[i]-corelset1)
                                corelset[i].intersection_update(corelset1)
                                corelset[i].update(difnew)
                                corelset[i].update(difold)
                                towrite = {}
                                le = len(difold)
                                si = len(coldicts[i])
                                for l,j in enumerate(difnew):
                                    if l<le:
                                        towrite[j] = coldicts[i][difold[l]]
                                    else:
                                        towrite[j] = si
                                        si+=1

                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset[i])))
                                coldict = coldicts[i]
                                s = zip(*towrite.keys())
                                if len(s)>0:
                                    output.write(zlib.compress(marshal.dumps(s[0],2)))
                                    output.write(zlib.compress(marshal.dumps(s[1],2)))
                                if len(corelset[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset[i]] ).tostring()))
                        elif i in correlated :
                                    pass
                        else:
                            difnew = list(setcol[i]-set(prevsets[i]))
                            difold = list(set(prevsets[i])-setcol[i])
                            s = []

                            s = list(prevsets[i]) + difnew
                            d = 0
                            while len(s)>dictsize:
                                s.remove[difold[d]]
                                d+=1
                            prevsets[i] = s




                            towrite = {}
                            #le = len(difold)
                            si = len(coldicts[i])
                            for ki,j in enumerate(difnew):
                                towrite[j] = si
                                si+=1

                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'

                                output.write(zlib.compress(marshal.dumps(towrite.keys(),2)))
                                l[0] = output.tell()
                                output.write(zlib.compress(array(indextype,towrite.values()).tostring()))
                                l[1] = output.tell()
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            output.write(zlib.compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[2] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[2])
#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(l.tostring()))



                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count

                count=0
                output.flush()
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                print 'lala' + str(fileIter.tell())
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                #listofarrays = []
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1

            if ret:
                fileIter.close()
                break


    if mode == 'spac':
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)
        setcol = [set([]) for _ in xrange(colnum)]
#        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
#        compressorlistvals = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        listoflens = [0 for _ in xrange(colnum)]
        start = 0
        dictsize = 65536
        paxcols = []
        indextype = 'H'
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        numrows=0
        dictlimit = step - colnum
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [[] for _ in xrange(colnum)]
        corelset = []
        count = 0
        blocknum = 0
        bsize = 0
        rowlimit = 0
        compress = zlib.compress

        while True:
            maxlen = 0.5
            ret = False
            z = zip(*itertools.islice(diter, 0, step))

            if z==[]:
                ret = True
            else:
                if blocknum==0:
                    for c in z:
                        for v in c:

                            bsize += getSize(v)

                listofnditers.append(z)


                if len(listofnditers[-1])!=0:
                    count += len(listofnditers[-1][0])
                else:
                    ret = 1

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l


                if blocknum>1:
                    listofvals = []
                    listofvals.append([val for subl in [x[paxcols[0]] for x in listofnditers] for val in subl])
                    if len(listofvals[0]) > rowlimit:
                        bsize = 300000001


            step = dictsize - maxlen
            if step < stopstep or ret or bsize>30000000:

                bsize=0
                prev = fileIter.tell() + 8*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]
                listofvals = []
                for j in xrange(colnum):
                    listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])

                if blocknum == 0:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev

#                        s =  [val for subl in [x[i] for x in listofnditers] for val in subl]
#                        if (dictsize*2+len(marshal.dumps(setcol[i],2))>len(marshal.dumps(s,2))):
#                            paxcols.append(i)
#                            output.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                        if (len(setcol[i])*1.0/maxlen>0.67):
                            paxcols.append(i)
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(listofvals[i],2)))
                            l[0] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])

                            #listofarrays.append(s)
#                        elif i==8 or i==9:
#                            if i==8:
#                                pass
#                            if i==9:
#                                corellated = zip(listofvals[8],listofvals[9])
#                                corelset = set(corellated)
#                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset)))
#                                coldict = coldicts[i]
#                                s = zip(*corelset)
#                                output.write(zlib.compress(marshal.dumps(s[0],2)))
#                                output.write(zlib.compress(marshal.dumps(s[1],2)))
#                                indextype='B'
#                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset] ).tostring()))
                        else:
                            prevsets[i] = sorted(list(set(setcol[i]).copy()))
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i])<256:
                                indextype='B'
                            else:
                                indextype='H'
                            #listofarrays.append(array(indextype,[coldict[val] for subl in [x[i] for x in listofnditers] for val in subl]))
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(prevsets[i],2)))
                            l[0] = output.tell()
                            output.write(compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[1] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[1])
                else:
#                    listofnditers1 = zip(*listofnditers)
#                    listofnditers1.sort(key=lambda listofnditers1: listofnditers1[paxcols[0]])
#                    listofnditers = zip(*listofnditers1)
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(listofvals[i],2)))
                            l[0] = output.tell() 
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])
                            if rowlimit==0:
                                rowlimit = len(listofvals[i])
                            
                            #listofarrays.append(s)
#                        elif i==8 or i==9:
#                            if i==8:
#                                pass
#                            if i==9:
#                                corellated = zip(listofvals[8],listofvals[9])
#                                corelset1 = set(corellated)
#                                difnew = list(corelset1-corelset)
#                                difold = list(corelset-corelset1)
#                                corelset.intersection_update(corelset1)
#                                corelset.update(difnew)
#                                corelset.update(difold)
#                                towrite = {}
#                                le = len(difold)
#                                si = len(coldicts[i])
#                                for l,j in enumerate(difnew):
#                                    if l<le:
#                                        towrite[j] = coldicts[i][difold[l]]
#                                    else:
#                                        towrite[j] = si
#                                        si+=1
#
#                                coldicts[i] = dict(((x,y) for y,x in enumerate(corelset)))
#                                coldict = coldicts[i]
#                                s = zip(*towrite.keys())
#                                if len(s)>0:
#                                    output.write(zlib.compress(marshal.dumps(s[0],2)))
#                                    output.write(zlib.compress(marshal.dumps(s[1],2)))
#                                indextype='B'
#                                output.write(zlib.compress(array(indextype,[coldict[val] for val in corelset] ).tostring()))
                        else:

                            pset = set(prevsets[i])
                            difnew = list(setcol[i] - pset)

                            s = []
                            
                            s = prevsets[i] + difnew
                            d = 0
                            if len(s) > dictsize:
                                difold = list(pset - setcol[i])

                                while len(s)>dictsize:
                                    s.remove[difold[d]]
                                    d+=1
                                    
                            prevsets[i] = s
                            towritevalues = (x for x in xrange(len(coldicts[i]), len(coldicts[i]) + len(difnew)))
                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            l = [0 for _ in xrange(3)]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'
                                if len(difnew)>10:
                                    difnew, towritevalues = zip(*sorted(zip(difnew, towritevalues)))
                                output.write(compress(marshal.dumps(difnew,2)))
                                l[0] = output.tell()
                                output.write(compress(array(indextype,towritevalues).tostring()))
                                l[1] = output.tell()
                            output.write(compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[2] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[2])
#                for i,l in enumerate(listofarrays):
#                    if i in paxcols:
#                        output.write(zlib.compress(marshal.dumps(sorted(l),2)))
#                    else:
#                        output.write(zlib.compress(l.tostring()))


                        
                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count
                
                count=0
                output.flush()
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.write(output.getvalue())
                listoflens = [0 for _ in xrange(colnum)]
                for s in setcol:
                    s.clear()
                #listofarrays = []
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1
                
            if ret:
                fileIter.close()
                break
            
            

    


    if mode == 'sorteddictpercol': #to do (arxika epilegw ta rows metrwntas ta sets meta ta vazw se sorted dict kai telos grafw tis kolwnes)
        colnum = len(schema)
        marshal.dump(schema,fileIter,2)

        setcol = [set([]) for _ in xrange(colnum)]
        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        compressorlistcols = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        start = 0
        step = 65535
        bsize = 0
        numrows=0
        dictlimit = step - colnum
        current = [[] for _ in xrange(colnum)]
        count = 0
        while True:
            maxlen = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            if len(nditer)!=0:
                count += len(nditer[0])
            for i,col in enumerate(nditer):
                current[i] += col
                setcol[i].update(col)
                l = len(setcol[i])
                if l > maxlen:
                    maxlen = l
            step = 65535 - maxlen
            if step < 5000 or len(nditer) == 0:
                prev = fileIter.tell()
                headindex = [0 for _ in xrange(colnum+2)]
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                for i in xrange(colnum):
                    headindex[i] = fileIter.tell()
                    s = sorted(setcol[i])
                    coldict = dict(((x,y) for y,x in enumerate(s)))
                    fileIter.write(compressorlistdicts[i].compress(marshal.dumps(s,2)))
                    fileIter.write(compressorlistcols[i].compress(array('H',[coldict[y] for y in current[i]]).tostring()))
                headindex[i+1] = fileIter.tell()
                headindex[i+2] = count
                count=0
                fileIter.seek(prev)
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.seek(headindex[colnum])
                current = [[] for _ in xrange(colnum)]
                setcol = [set([]) for _ in xrange(colnum)]
                gc.collect()
                step = 65535
            if len(nditer)==0:
                break



    if mode == 'dictpercol':
        colnum = len(schema)
        fastPickler.dump(schema)
        listptr = [array('H') for _ in xrange(colnum) ]
        compressorlistdicts = [zlib.BZ2Compressor(9) for _ in xrange(colnum)]
        compressorcols = zlib.BZ2Compressor(9)
        listofdicts = [{} for _ in xrange(colnum) ]
        listoflens = [-1 for _ in xrange(colnum)]
        start = 0
        bsize = 0
        numrows=0
        dictlimit = 65536 - colnum
        for row in diter:
            numrows+=1

            for i,val in enumerate(row):
                if val in listofdicts[i]:
                    listptr[i].append(listofdicts[i][val])
                else:
                    listoflens[i] += 1
                    listofdicts[i][val] = listoflens[i]
                    listptr[i].append(listoflens[i])
                    if type(val) in (unicode,str):
                        bsize += len(val)
            if bsize>BLOCK_SIZE  or max(listoflens) >= dictlimit:
                prev = fileIter.tell()
                headindex = [0 for _ in xrange(colnum+3)]
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                for i,valset in enumerate(listofdicts):
                    headindex[i] = fileIter.tell()
                    fileIter.write(compressorlistdicts[i].compress(marshal.dumps(sorted(valset, key=valset.get))))
                headindex[colnum] = fileIter.tell()
                for ar in listptr:
                    fileIter.write(compressorcols.compress(ar.tostring()))
                headindex[colnum+1] = fileIter.tell()
                headindex[colnum+2] = numrows
                fileIter.seek(prev)
                fileIter.write(struct.pack('L'*len(headindex), *headindex))
                fileIter.seek(headindex[colnum+1])
                listofdicts = [{} for _ in xrange(colnum) ]
                listoflens = [-1 for _ in xrange(colnum)]
                bsize=0
                numrows=0
                listptr = [array('H') for _ in xrange(colnum) ]
                gc.collect()
        prev = fileIter.tell()
        headindex = [0 for _ in xrange(colnum+3)]
        fileIter.write(struct.pack('L'*len(headindex), *headindex))
        for i,valset in enumerate(listofdicts):
            headindex[i] = fileIter.tell()
            fileIter.write(compressorlistdicts[i].compress(marshal.dumps(sorted(valset, key=valset.get))))
        headindex[colnum] = fileIter.tell()
        for ar in listptr:
            fileIter.write(compressorcols.compress(ar.tostring()))
        headindex[colnum+1] = fileIter.tell()
        fileIter.seek(prev)
        headindex[colnum+2] = numrows
        fileIter.write(struct.pack('L'*len(headindex), *headindex))





    if mode == 'valdictcols1':
        colnum = len(schema)
        cPickle.dump(schema, fileIter,1)
        valset = {}
        lenvalset = 0
        listptr = [array('H') for _ in xrange(colnum) ]
        listptrappend = [listptr[i].append for i in xrange(colnum)]
        start = 0
        bsize = 0
        dictlimit = 65536-colnum
        count=0
        vdefault = valset.setdefault

        for row in diter:


            for i,app in enumerate(listptrappend):
                d = vdefault(row[i], lenvalset)
                app(d)
                if d == lenvalset:
                    lenvalset += 1
                    count+=1

#            for i,val in enumerate(row):
#                if val in valset:
#                    listptrappend[i](valset[val])
#                else:
#                    listptrappend[i](lenvalset)
#                    valset[val] = lenvalset
#                    lenvalset += 1
#                    count += 1


#                d = vdefault(val, lenvalset)
#                if d == lenvalset:
#                    lenvalset += 1
#                    count+=1
#                listptrappend[i](d)

#

#                    if type(val) in (unicode,str):
#                        bsize += len(val)

            if count == 2048:
                #bsize = sys.getsizeof(valset.keys())
                count = 0
                        
            if bsize>BLOCK_SIZE  or lenvalset >= dictlimit:
                fastPickler.dump(sorted(valset.keys()))
#                fastPickler.dump(listptr)
                for ar in listptr:
                    ar.tofile(fileIter)
           #     fileIter.write( struct.pack(fmt * len(listptr[0]), *(j for i in listptr for j in i)))
                valset.clear()
                lenvalset = 0
                bsize=0
                listptr = [array('H') for _ in xrange(colnum) ]
                gc.collect()
        fastPickler.dump(sorted(valset.keys()))
#        fastPickler.dump(listptr)
        #fileIter.write( struct.pack(fmt * len(listptr[0]), *(j for i in listptr for j in i)))
        for ar in listptr:
            ar.tofile(fileIter)

            

    if mode == 'valdictcols':
        try:
            colnum = len(schema)
            cPickle.dump(schema, fileIter,1)
            valset = {}
            lenvalset = -1
            listptr = [[] for _ in xrange(colnum) ]
            start = 0
            step = 1024
            currentblock = fileIter.tell()
            count = 0
            bsize = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            gc.disable()
            while True:
                check = 0
                i = -1
                for col in nditer:
                    i+=1
                    check=1
                    for val in col:
                        if val in valset:
                            listptr[i].append(valset[val])
                        else:
                            lenvalset += 1
                            valset[val] = lenvalset
                            listptr[i].append(lenvalset)
                    bsize += sys.getsizeof(col)
                count += step
                if bsize>BLOCK_SIZE or count>=(65536-1024)/colnum or check == 0:
                    fastPickler.dump(sorted(valset, key=valset.get))
                    fileIter.write( struct.pack('H' * (len(listptr[0])*colnum), *(j for i in listptr for j in i)))
                    valset = {}
                    lenvalset = -1
                    count = 0
                    listptr = [[] for _ in xrange(colnum)]
                    if check==0:
                        break

                nditer = izip(*itertools.islice(diter, start, step))

            gc.enable()
        except StopIteration,e:
            gc.enable()
            pass

    if mode == 'valdictrows':
        try:
            colnum = len(schema)
            cPickle.dump(schema, fileIter,1)
            valset = {}
            lenvalset = -1
            listptr = []
            start = 0
            step = 1024
            currentblock = fileIter.tell()
            count = 0
            bsize = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            gc.disable()
            while True:
                check = 0
                i = 0
                for col in nditer:
                    i += 1
                    check=1
                    colcount = -1
                    for val in col:
                        colcount+=1
                        if  i == 1:
                            listptr.append([])
                        if val in valset:
                            listptr[count + colcount].append(valset[val])
                        else:
                            lenvalset += 1
                            valset[val] = lenvalset
                            listptr[count + colcount].append(lenvalset)
                    bsize += sys.getsizeof(col)
                count += step
                if bsize>BLOCK_SIZE or count>=65536 or check == 0:
                    fastPickler.dump(sorted(valset, key=valset.get))
                    fileIter.write( struct.pack('i' * (len(listptr)*colnum), *(j for i in listptr for j in i)))
                    valset = {}
                    lenvalset = -1
                    count = 0
                    listptr = []
                    if check==0:
                        break

                nditer = izip(*itertools.islice(diter, start, step))

            gc.enable()
        except StopIteration,e:
            gc.enable()
            pass

    if mode == 'topdict':
        try:
            colnum = len(schema)
            cPickle.dump(schema, fileIter,1)
            todisk = [{} for _ in xrange(colnum)]
            start = 0
            step = 2048/colnum
            currentblock = fileIter.tell()
            recnum = 0
            bsize = 0
            while True:
                notmlist=0
                temp = []
                while bsize < BLOCK_SIZE  and recnum!=65536/colnum:
                    nditer = itertools.islice(diter, start, step)
                    mlist = list(nditer)
                    if not mlist:
                        notmlist=1
                    #bsize += sys.getsizeof(mlist)
                    recnum += step
                    mlist = mlist+temp
                    temp = mlist
                valset = OrderedDict()
                lenvalset = -1
                listptr = []
                rowcount = 0
                for row in mlist:
                    colcount=0
                    listptr.append([])
                    for col in row:
                        if col in valset:
                            listptr[rowcount].append(valset[col])
                        else:
                            lenvalset += 1
                            valset[col] = lenvalset
                            listptr[rowcount].append(lenvalset)
                        colcount+=1
                    rowcount+=1

                fastPickler.dump(list(valset.keys()))
                fileIter.write( struct.pack('I' * (len(listptr)*colnum), *(j for i in listptr for j in i)))
                bsize = 0
                recnum = 0
                if notmlist==1:
                    break


        except StopIteration,e:
            pass



    if mode == 'storeindict':
        try:
            colnum = len(schema)
            cPickle.dump(schema, fileIter,1)
            todisk = [{} for _ in xrange(colnum)]
            start = 0
            step = 1024
            currentblock = fileIter.tell()
            count = 0
            bsize = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            gc.disable()
            while True:
                i = 0
                check = 0
                for col in nditer:
                    check=1
                    valid=0
                    for val in col:
                        try:
                            todisk[i][val].append(valid)
                        except:
                            todisk[i][val] = [valid]
                        valid+=1
                    bsize += sys.getsizeof(col)
                    i+=1
                    count += step
                if bsize>BLOCK_SIZE or count==32768:
                    output = cStringIO.StringIO()
                    fastPickler = cPickle.Pickler(output, 1)
                    prev = 0
                    fastPickler.fast = 1
                    index_loc = currentblock + (colnum+1)*8
                    index = [0 for _ in xrange(colnum+1)]
                    ind = -1
                    for col in todisk:
                        ind += 1
                        index[ind] = prev + index_loc
                        fastPickler.dump(col)
                        prev = output.tell()
                    index[ind+1] = prev + index_loc
                    currentblock = prev+index_loc
                    fileIter.write(struct.pack('L'*len(index), *index))
                    fileIter.write(output.getvalue())
                    todisk = [{} for _ in xrange(len(schema))]
                    count = 0
                    bsize = 0

                nditer = izip(*itertools.islice(diter, start, step))
                if check == 0:
                    output = cStringIO.StringIO()
                    fastPickler = cPickle.Pickler(output, 1)
                    prev = 0
                    fastPickler.fast = 1
                    index_loc = currentblock + (colnum+1)*8
                    index = [0 for _ in xrange(colnum+1)]
                    ind = -1
                    for col in todisk:
                        ind += 1
                        index[ind] = prev + index_loc
                        fastPickler.dump(col)
                        prev = output.tell()
                    index[ind+1] = prev + index_loc
                    currentblock = prev+index_loc
                    fileIter.write(struct.pack('L'*len(index), *index))
                    fileIter.write(output.getvalue())
                    break

            gc.enable()
        except StopIteration,e:
            gc.enable()
            pass

    def spac(fileObject):
        colnum = len(schema)-1
        marshal.dump(schema[1:],fileObject,2)
        setcol = [set([]) for _ in xrange(colnum)]
        dictsize = 65536
        paxcols = []
        indextype = 'H'
        index_init = [0 for _ in xrange(3)]
        step = dictsize
        stopstep = int(dictsize/20)
        bsize = 0
        listofnditers = []
        coldicts = [{} for _ in xrange(colnum)]
        prevsets =  [[] for _ in xrange(colnum)]
        count = 0
        blocknum = 0
        bsize = 0
        rowlimit = 0
        compress = zlib.compress
        

        while True:
            maxlen = 0
            exitGen = False
            rows = []
            try:
                for i in xrange(step):
                    rows.append((yield))
            except GeneratorExit:
                exitGen = True
            z = zip(*rows)

            if z!=[]:
                if blocknum==0:
                    for c in z:
                        for v in c:
                            bsize += getSize(v)
                listofnditers.append(z)
                count += len(listofnditers[-1][0])
                

                for i,col in enumerate(listofnditers[-1]):
                    if i not in paxcols:
                        setcol[i].update(col)
                        l = len(setcol[i])
                        if l > maxlen:
                            maxlen = l

                if blocknum>=1:
                    listofvals = []
                    listofvals.append([val for subl in [x[paxcols[0]] for x in listofnditers] for val in subl])
                    if len(listofvals[0]) > rowlimit:
                        bsize = 30000001


            step = dictsize - maxlen
            if step < stopstep or exitGen or bsize>30000000:
                bsize=0
                prev = fileObject.tell() + 8*(colnum+2)
                output = cStringIO.StringIO()
                headindex = [0 for _ in xrange(colnum+2)]
                listofvals = []
                for j in xrange(colnum):
                    listofvals.append([val for subl in [x[j] for x in listofnditers] for val in subl])

                if blocknum == 0:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if (len(setcol[i])*1.0/maxlen>0.67):
                            paxcols.append(i)
                            l = index_init[:]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(listofvals[i],2)))
                            l[0] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])
                            if rowlimit == 0:
                                rowlimit = len(listofvals[i])
                        else:
                            prevsets[i] = list(set(setcol[i]).copy())
                            coldicts[i] = dict(((x,y) for y,x in enumerate(prevsets[i])))
                            coldict = coldicts[i]
                            if len(prevsets[i])<256:
                                indextype='B'
                            else:
                                indextype='H'
                            l = index_init[:]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(prevsets[i],2)))
                            l[0] = output.tell()
                            output.write(compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[1] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[1])
                else:
                    for i in xrange(colnum):
                        headindex[i] = output.tell() + prev
                        if i in paxcols:
                            l = index_init[:]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            output.write(compress(marshal.dumps(listofvals[i],2)))
                            l[0] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[0])
                            
                        else:
                            pset = set(prevsets[i])
                            difnew = list(setcol[i] - pset)
 #                           s = []
                            s = prevsets[i] + difnew
                            d = 0
                            if len(s) > dictsize:
                                difold = list(pset - setcol[i])
                                while len(s)>dictsize:
                                    s.remove[difold[d]]
                                    d+=1

                            prevsets[i] = s
                            towritevalues = (x for x in xrange(len(coldicts[i]), len(coldicts[i]) + len(difnew)))

                            coldicts[i] = dict(((x,y) for y,x in enumerate(s)))
                            coldict = coldicts[i]
                            l = index_init[:]
                            t = output.tell()
                            output.write(struct.pack('L'*len(l), *l))
                            if len(prevsets[i]) != 0 :
                                if len(prevsets[i])<256:
                                    indextype='B'
                                else:
                                    indextype='H'

                                output.write(compress(marshal.dumps(difnew,2)))
                                l[0] = output.tell()
                                output.write(compress(array(indextype,towritevalues).tostring()))
                                l[1] = output.tell()
                            output.write(compress(array(indextype,[coldict[val] for val in listofvals[i]] ).tostring()))
                            l[2] = output.tell()
                            output.seek(t)
                            output.write(struct.pack('L'*len(l), *l))
                            output.seek(l[2])

                headindex[i+1] = output.tell()+ prev
                headindex[i+2] = count
                count=0
                fileObject.write(struct.pack('L'*len(headindex), *headindex))
                fileObject.write(output.getvalue())
                for s in setcol:
                    s.clear()
                listofnditers = []
                gc.collect()
                step = dictsize
                blocknum+=1

            if exitGen:
                fileObject.close()
                break


    def rcfile(fileObject):
        colnum = len(schema) - 1
        structHeader = 'L' * (len(schema)+1)
        indexinit = [0 for _ in xrange(len(schema)+1)]
        marshal.dump(schema[1:], fileObject,2)
        todisk = [[] for _ in xrange(colnum)]
        start = 0
        step = 1024
        currentblock = fileObject.tell()
        bsize = 0
        rows = []
        exitGen = False
        lencols = 0
        blocknum = 0
        while not exitGen:
            #nditer = zip(*itertools.islice(diter, 0, step))
            try:
                for i in xrange(step):
                    rows.append((yield))
            except GeneratorExit:
                exitGen = True

            nditer = zip(*rows)

            i = 0
            check = 0
            for col in nditer:
                check=1
                todisk[i] += col
                if blocknum == 0:
                    for val in col:
                        bsize += getSize(val)
                elif i == 0 and len(todisk[0]) >= lencols:
                    bsize = BLOCK_SIZE+1
                i+=1
            if bsize>BLOCK_SIZE or exitGen:
                if blocknum==0:
                    lencols = len(todisk[0])
                blocknum += 1
                index = indexinit[:]
                ind = -1
                output = cStringIO.StringIO()
                indi = fileObject.tell()
                output.write(struct.pack(structHeader, *index))
                for i,col in enumerate(todisk):
                    ind += 1
                    index[ind] = output.tell()+indi
                    output.write(zlib.compress(marshal.dumps(col,2)))
                    index[ind+1] = output.tell()+indi
                    if exitGen:
                        index[colnum+1] = 1
                output.seek(0)
                output.write(struct.pack(structHeader, *index))
                fileObject.write(output.getvalue())
                todisk = [[] for _ in xrange(len(schema)-1)]
                bsize = 0
            rows = []
        fileObject.close()

    def rcfilenonsplit(fileObject=fileIter,colnum = (len(schema))):
            structHeader = 'L' * (colnum+2)
            indexinit = [0 for _ in xrange(colnum+2)]
            marshal.dump(schema[len(schema)-colnum:], fileObject,2)
            todisk = [[] for _ in xrange(colnum)]
            start = 0
            step = 1024
            currentblock = fileObject.tell()
            bsize = 0
            lencols = 0
            blocknum = 0
            exitGen = 1
            while exitGen:
                nditer = izip(*itertools.islice(diter, start, step))
                i = 0
                exitGen = 0
                for col in nditer:
                    exitGen=1
                    todisk[i] += col
                    if blocknum == 0:
                        for val in col:
                            bsize += getSize(val)
                    elif i == 0 and len(todisk[0]) >= lencols:
                        bsize = BLOCK_SIZE+1

                    i+=1
                if bsize>BLOCK_SIZE or not exitGen:
                    index = indexinit[:]
                    lencols = len(todisk[0])
                    blocknum += 1
                    ind = -1
                    output = cStringIO.StringIO()
                    indi = fileObject.tell()
                    output.write(struct.pack(structHeader, *index))
                    for i,col in enumerate(todisk):
                        ind += 1
                        index[ind] = output.tell()+indi
                        output.write(zlib.compress(marshal.dumps(col,2)))
                        index[ind+1] = output.tell()+indi
                        if not exitGen:
                            index[colnum+1] = 1
                    output.seek(0)
                    output.write(struct.pack(structHeader, *index))
                    fileObject.write(output.getvalue())
                    todisk = [[] for _ in xrange(len(schema))]
                    bsize = 0


    if mode == 'spac1':
        if 'split' in formatArgs:
            filesNum = int(formatArgs['split'])
            filesList = [None]*filesNum
            for key in xrange(int(formatArgs['split'])) :
                filesList[key] = open(os.path.join(fullpath, filename+'.'+str(key)), 'wb')

            spacgen = [spac(x) for x in filesList]
            spacgensend = [x.send for x in spacgen]
            for j in spacgensend:
                j(None)
            for row in diter:
                spacgensend[row[0]](row[1:])
            for j in spacgen:
                j.close()
        else :
            rcfilenonsplit()

    if mode == 'rcfile':
        if 'split' in formatArgs:
            filesNum = int(formatArgs['split'])
            filesList = [None]*filesNum
            for key in xrange(int(formatArgs['split'])) :
                filesList[key] = open(os.path.join(fullpath, filename+'.'+str(key)), 'wb')

            rcgen = [rcfile(x) for x in filesList]
            rcgensend = [x.send for x in rcgen]
            for j in rcgensend:
                j(None)
            for row in diter:
                rcgensend[row[0]](row[1:])
            for j in rcgen:
                j.close()
        else :
            rcfilenonsplit()
    


    if mode == 'itertools':
        step = 1024
        while True:
            check = 0
            for col in diter:
                check = 1
                break
            if check == 0:
                break

    if mode == 'deduplipax':
        try:
            colnum = len(schema)
            cPickle.dump(schema, fileIter,1)
            todisk = [[] for _ in xrange(colnum)]
            start = 0
            step = 1024
            compr = zlib.BZ2Compressor(9)
            currentblock = fileIter.tell()
            myD=[{} for _ in xrange(colnum)]
            bsize = 0
            nditer = zip(*itertools.islice(diter, 0, step))
            
            while True:
                i = 0
                for col in nditer:
                    bsize += sys.getsizeof(col)
                    #for val in col:
                    #    todisk[i].append(myD.setdefault(val,val))
#                    todisk[i] += col
                    todisk[i] += [myD[i].setdefault(val,val) for val in col]
                    i+=1
                if bsize>BLOCK_SIZE:
                    output = cStringIO.StringIO()
                    fastPickler = cPickle.Pickler(output, 1)
                    prev = 0
                    #fastPickler.fast = 1
                    index_loc = currentblock + (colnum+1)*8
                    index = [0 for _ in xrange(colnum+1)]
                    ind = -1
                    for col in todisk:
                        ind += 1
                        index[ind] = prev + index_loc
                        output.write(compr.compress(cPickle.dumps(col)))
                        prev = output.tell()
                    index[ind+1] = prev + index_loc
                    currentblock = prev+index_loc
                    fileIter.write(struct.pack('L'*len(index), *index))
                    fileIter.write(output.getvalue())
                    todisk = [[] for _ in xrange(len(schema))]
                    count = 0
                    bsize = 0
                if not nditer:
                    output = cStringIO.StringIO()
                    fastPickler = cPickle.Pickler(output, 1)
                    prev = 0
                    #fastPickler.fast = 1
                    index_loc = currentblock + (colnum+1)*8
                    index = [0 for _ in xrange(colnum+1)]
                    ind = -1
                    for col in todisk:
                        ind += 1
                        index[ind] = prev + index_loc
                        output.write(zlib.compress(cPickle.dumps(col),9))
                        prev = output.tell()
                    index[ind+1] = prev + index_loc
                    currentblock = prev+index_loc
                    fileIter.write(struct.pack('L'*len(index), *index))
                    fileIter.write(output.getvalue())
                    todisk = [[] for _ in xrange(len(schema))]
                    count = 0
                    bsize = 0
                    break
                else:
                    nditer = zip(*itertools.islice(diter, start, step))
               
            
        except StopIteration,e:
            
            pass

    if mode == 'row':   # rowstore periptwsi
        todump = []
        try:
            bsize = 0
            fastPickler = cPickle.Pickler(fileIter, 1)
            fastPickler.fast = 1
            for row in diter:
                if bsize>BLOCK_SIZE:
                    fastPickler.dump(todump)
                    todump = [row]
                    bsize += len(marshal.dumps(row))
                else:
                    todump.append(row)
                    bsize += len(marshal.dumps(row))
        except StopIteration,e:
            pass
        if len(todump)>0:
             fastPickler.dump(todump)

    try:
        if 'split' not in formatArgs:
            fileIter.close()
    except NameError:
        pass

boolargs=lib.inoutparsing.boolargs+['compression']


def Source():
    global boolargs, nonstringargs
    return SourceNtoOne(outputData,boolargs, lib.inoutparsing.nonstringargs,lib.inoutparsing.needsescape, connectionhandler=True)


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
