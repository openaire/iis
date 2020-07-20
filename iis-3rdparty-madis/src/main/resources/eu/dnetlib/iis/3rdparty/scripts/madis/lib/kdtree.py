import operator
import itertools
from collections import deque
import math

CONSTRAINT_EQ = 2
CONSTRAINT_GT = 4
CONSTRAINT_LE = 8
CONSTRAINT_LT = 16
CONSTRAINT_GE = 32
LEFT = 0
RIGHT = 1
EXCLUSIVE =0 
SMALLEST = -1
BIGGEST = 1
NORMAL=0
ENTIRERANGE = [(SMALLEST, None, LEFT), (BIGGEST, None, RIGHT)]
EMPTYRANGE= None
INFPOS=(BIGGEST, None, RIGHT)
INFNEG=(SMALLEST, None, LEFT)

class Tree:pass

def kdtree(data,cols=None):
    if not data:
        return

    if not cols:
        try:
            cols=range(len(data[0]))
        except KeyboardInterrupt:
            raise           
        except:
            cols=[0]
    k = len(cols)
    if k==1:
        data = sorted(data, key=operator.itemgetter(cols[0]))

    treedata=[None]*(2**int(math.ceil(math.log(len(data),2))+1))

    kdtreerec(data,treedata,cols,0,0)
    tree=Tree()
    tree.columns=cols
    tree.data=treedata
    tree.numberofdata=len(data)
    return tree

def kdtreerec(data, treedata, cols, axis, index):
    lendata= len(data)
    median = lendata>>1 # choose median
    k=len(cols)
    if k!=1:
        data = sorted(data, key=operator.itemgetter(cols[axis]))

    treedata[index]=data[median]
    axis=axis+1
    if axis==k:
        axis=0

    if lendata!=1:
        left=data[:median]
        right=data[median:]

        del(data)
        
        kdtreerec(left, treedata, cols, axis, (index<<1)+1)
        kdtreerec(right, treedata, cols, axis, (index+1)<<1)
    return

def query(tree, constraints=None, consargs=None):
    if tree == None:
        return

    columns=tree.columns
    treedata=tree.data
    numberofdata=tree.numberofdata

    k=len(columns)
    cc={}
    if constraints!=None and consargs!=None:
        cc=compineconstraints(constraints, consargs)
    ranges=[]
    for c in columns:
        if c not in cc:
            ranges.append(ENTIRERANGE)
        else:
            ranges.append(cc[c])
    if cc==None:
        return

    to_visit=deque()
    treedatalen=len(treedata)
    direct=True
    index=axis=0
    while direct or len(to_visit)!=0:
        if not direct:
            index,axis = to_visit.pop()
        else:
            direct=False
        row=treedata[index]
        leftindex=(index<<1)+1

        if leftindex>treedatalen or (leftindex>=numberofdata and leftindex<treedatalen and treedata[leftindex]==None):
            passes=True
            for i in xrange(k):
                if not ranges[i][0]<=(NORMAL, row[columns[i]],0)<ranges[i][1]:
                    passes=False
                    break
            if passes:
                yield row
            continue

        middle=row[columns[axis]]
        newaxis=axis+1
        if newaxis==k: newaxis=0

        if ranges[axis][1]>(NORMAL, middle,0):
            index=leftindex+1
            direct=True
        if ranges[axis][0]<=(NORMAL, middle,0):
            if direct:
                to_visit.append((index , newaxis))
            else:
                direct=True
            index=leftindex
        axis=newaxis

def compineconstraints(constraints, consargs):
    consdict={}
    i=0
    ri=None
    for c in constraints:
        consaxis, constype = c
        if consaxis not in consdict:
            consdict[consaxis]=constrainttorange(constype, consargs[i])
        else:
            ri=intersectranges(consdict[consaxis], constrainttorange(constype, consargs[i]))
            if ri==None:
                return None
            consdict[consaxis]=ri
        i=i+1

    return consdict

def intersectranges(range1, range2):
    if EMPTYRANGE in range1 or EMPTYRANGE in range2 or range1[0] >= range1[1] or range2[0] >= range2[1]:
        return EMPTYRANGE
    maxofmin = max(range1[0], range2[0])
    minofmax = min(range1[1], range2[1])
    if maxofmin>=minofmax:
        return EMPTYRANGE
    return [maxofmin, minofmax]

def constrainttorange(constype, consarg):
    if constype<=CONSTRAINT_LE:
        if constype==CONSTRAINT_LE:
            return [INFNEG, (NORMAL, consarg, RIGHT)]
        if constype==CONSTRAINT_EQ:
            return [(NORMAL,consarg, LEFT), (NORMAL,consarg, RIGHT)]
        else: #if constype==CONSTRAINT_GT:
            return [(NORMAL,consarg, RIGHT), INFPOS]
    else:
        if constype==CONSTRAINT_GE:
            return [(NORMAL,consarg, LEFT), INFPOS]
        else: #if constype==CONSTRAINT_LT:
            return [INFNEG, (NORMAL, consarg, LEFT)]

if __name__ == "__main__":
    print intersectranges([(NORMAL,5,LEFT),INFPOS], [(NORMAL,5,RIGHT), INFPOS])
    print intersectranges([(NORMAL,5,LEFT),INFPOS], [(NORMAL,2,RIGHT), (NORMAL,7,RIGHT)])
    print intersectranges([INFNEG,(NORMAL,5,LEFT)], [(NORMAL,2,RIGHT), (NORMAL,7,RIGHT)])
    print intersectranges([INFNEG,(NORMAL,5,RIGHT)], [(NORMAL,5,LEFT), INFPOS])
    print intersectranges([INFNEG,(NORMAL,'a',RIGHT)], [(NORMAL,'a',LEFT), INFPOS])
    print intersectranges([INFNEG,(NORMAL,'a',LEFT)], [(NORMAL,'a',RIGHT), INFPOS])
    print intersectranges([INFNEG,(NORMAL,'a',LEFT)], ENTIRERANGE)
    print intersectranges([INFNEG,(NORMAL,0,LEFT)], ENTIRERANGE)

    print constrainttorange(CONSTRAINT_EQ, 2)
    print constrainttorange(CONSTRAINT_LE, 0)
    print constrainttorange(CONSTRAINT_GE, 0)
    r1=intersectranges(constrainttorange(CONSTRAINT_LE, 0), ENTIRERANGE)
    print intersectranges(r1, constrainttorange(CONSTRAINT_GE, 0))

    print "----------------------"
    data=[[3],[2],[1],[4],[5],[5]]
    print "DATA:",data
    ks1=kdtree(data)
    print ks1.data

    print list(query(ks1))
#    print query(ks1,[(0,CONSTRAINT_GE),(0,CONSTRAINT_LT)], [3,5]) ]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE),(0,CONSTRAINT_LE)], [3,5]) ]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GT),(0,CONSTRAINT_LT)], [3,3]) ]
#    print 'q',[data[i] for i in query(ks1,[(0,CONSTRAINT_EQ)], [3]) ]

    data=[(5, 3), (5, 2), (5, 1), (2, 3), (2, 2), (2, 1)]
    print "DATA:",data
    ks1=kdtree(data,[1])
    print "TREE:",ks1.data
    print list(query(ks1))
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (0,CONSTRAINT_LT)], [3,4])]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (1,CONSTRAINT_LT)], [3,4])]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (1,CONSTRAINT_LT)], [3,5])]

    #print list(query(ks1,[ (0,CONSTRAINT_GE),(1,CONSTRAINT_LE)], [3,4]))
    print list(query(ks1,[ (1,CONSTRAINT_GE)], [2]))

    data=[[5]]
    print "DATA:",data
    ks1=kdtree(data)
    print "TREE:",ks1.data
    print list(query(ks1))
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (0,CONSTRAINT_LT)], [3,4])]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (1,CONSTRAINT_LT)], [3,4])]
#    print [data[i] for i in query(ks1,[(0,CONSTRAINT_GE), (1,CONSTRAINT_LT)], [3,5])]

    #print list(query(ks1,[ (0,CONSTRAINT_GE),(1,CONSTRAINT_LE)], [3,4]))

