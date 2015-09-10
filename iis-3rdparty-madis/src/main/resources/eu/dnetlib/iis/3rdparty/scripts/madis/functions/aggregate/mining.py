import re
import itertools
import setpath
import functions
import lib.jopts as jopts
from operator import itemgetter
import random

__docformat__ = 'reStructuredText en'

re_params=re.compile('(\w*):(.*)')

def consumer(func):
    """A decorator, advances func to its first yield point when called.
    """

    from functools import wraps

    @wraps(func)
    def wrapper(*args,**kw):
        gen = func(*args, **kw)
        gen.next()
        return gen
    return wrapper


class freqitemsets:
    """
    .. function:: freqitemsets(datacol, [threshold, noautothres, stats, maxlen]) -> [itemset_id:int, itemset_length:int, itemset_frequency:int, item:text]

    Calculates frequent itemsets on a given column (datacol). The algorithm is tuned for the
    case when we have many different items (in the order of millions), many input itemsets, but
    small itemset length (10-20).

    Returned table schema:

    :itemset_id: Automatic itemset id
    :itemset_length: Length of itemset
    :itemset_frequency: How many times an itemset has been found
    :item: Itemset's item value

    Parameters:

    :datacol:

        Column on which to calculate frequent itemsets

    :threshold: Default is 2

        How many times an freq. itemset must appear for it to appear in the results

    :noautothres: 1/0 (Default is 0)

        Do not calculate the threshold automatically

    :stats: 1/0 (Default is 0)

        Return frequent itemset statistics

    :maxlen: NUMBER (Default is no limit at all)

        Maximum itemset length to search

    Examples:
    
    >>> table1('''
    ... 'car wood bike' 'first group'
    ... 'car car wood'  'first group'
    ... 'car wood'      'first group'
    ... 'car wood ice'  'first group'
    ... 'ice'           'second group'
    ... 'car ice'       'second group'
    ... 'car cream toy' 'second group'
    ... 'icecream ice car toy'  'second group'
    ... ''')
    >>> sql("select b,freqitemsets(a, 'threshold:2', 'noautothres:1', 'maxlen:2') from table1 group by b")
    b            | itemset_id | itemset_length | itemset_frequency | item
    ---------------------------------------------------------------------
    first group  | 1          | 1              | 4                 | wood
    first group  | 2          | 1              | 4                 | car
    first group  | 3          | 2              | 4                 | car
    first group  | 3          | 2              | 4                 | wood
    second group | 1          | 1              | 3                 | ice
    second group | 2          | 1              | 3                 | car
    second group | 3          | 1              | 2                 | toy
    second group | 4          | 2              | 2                 | car
    second group | 4          | 2              | 2                 | ice
    second group | 5          | 2              | 2                 | car
    second group | 5          | 2              | 2                 | toy

    >>> sql("select b,freqitemsets(a, 'stats:1') from table1 group by b")
    b            | MaxTransactionLength | CombinationCount | PassedTransactions | ValidKeywords
    -------------------------------------------------------------------------------------------
    first group  | 3                    | 2                | 3                  | 2
    first group  | 3                    | 1                | 1                  | 2
    first group  | 3                    | 0                | 0                  | 0
    second group | 4                    | 3                | 3                  | 3
    second group | 4                    | 0                | 3                  | 0
    """


    registered=True
    multiset=True

    def __init__(self):
        self.threshold=2
        self.startingthreshold=2
        self.autothres=1
        self.compress=0
        self.initstatic=False
        self.input={}
        self.maxlength=0
        self.kwcode={}
        self.codekw={}
        self.maxkwcode=0
        self.overthres={}
        self.belowthres={}
        self.passedkw={}
        self.init=True
        self.itemset_id=0
        self.maxlen=None
        self.stats=False

    def initargs(self, args):
        self.init=False
        for i in xrange(1, len(args)):
            v=re_params.match(args[i])
            if v is not None and v.groups()[0]!='' and v.groups()[1]!='' and i>0:
                v=v.groups()
                if v[0]=='threshold':
                    try:
                        self.threshold=int(v[1])
                        self.startingthreshold=self.threshold
                    except KeyboardInterrupt:
                        raise               
                    except:
                        raise functions.OperatorError("FreqItemsets",'No integer value given for threshold')
                if v[0]=='noautothres':
                    self.autothres=0
                if v[0]=='compress':
                    self.compress=1
                if v[0]=='maxlen':
                    self.maxlen=int(v[1])
                if v[0]=='stats':
                    self.stats=True

    def demultiplex(self, data):
        iterable=None
        iterpos=-1

        for i in xrange(len(data)):
            if hasattr(data[i],'__iter__')==True:
                iterable=data[i]
                iterpos=i
                break

        if iterpos==-1:
            yield list(data)
        else:
            pre=list(data[0:iterpos])
            post=list(data[iterpos+1:])
            for i in iterable:
                if hasattr(i,'__iter__')==False:
                    yield pre+[i]+post
                else:
                    yield pre+list(i)+post
        
    def insertcombfreq(self, comb, freq):
        if comb in self.overthres:
            self.overthres[comb]+=freq
        else:
            if comb in self.belowthres:
                self.belowthres[comb]+=freq
            else:
                self.belowthres[comb]=freq

            if self.belowthres[comb]>=self.threshold:
                self.overthres[comb]=self.belowthres[comb]
                del(self.belowthres[comb])
                for k in comb:
                    if self.compress==0:
                        self.passedkw[k]=True
                    elif not k in self.passedkw:
                        self.passedkw[k]=self.overthres[comb]
                    else:
                        self.passedkw[k]+=self.overthres[comb]

    def insertitemset(self, itemset):
        if itemset not in self.input:
            self.input[itemset]=1
        else:
            self.input[itemset]+=1

    def cleanitemsets(self, minlength):
        newitemsets={}
        for k,v in self.input.iteritems():
            itemset=tuple(i for i in k if i in self.passedkw)
            if self.compress==1:
                esoteric_itemset=tuple(i for i in itemset if self.passedkw[i]==v)
                if len(esoteric_itemset)>0:
                    if len(itemset)>=minlength:
                        self.overthres[itemset]=v
                    itemset=tuple(i for i in itemset if self.passedkw[i]!=v)
            if len(itemset)>=minlength:
                if itemset not in newitemsets:
                    newitemsets[itemset]=v
                else:
                    newitemsets[itemset]+=v

        self.input=newitemsets

    def step(self, *args):
        if self.init==True:
            self.initargs(args)

        if len(args[0])==0:
            return
        
        itms=sorted(set(args[0].split(' ')))
        itms=[x for x in itms if x!='']
        li=len(itms)
        if li>0:
            if li>self.maxlength:
                self.maxlength=li

            inputkws=[]
            for kw in itms:
                if len(kw)==0:
                    print itms, args[0], len(args[0]), li
                if kw not in self.kwcode:
                    self.kwcode[kw]=self.maxkwcode
                    self.codekw[self.maxkwcode]=kw
                    inputkws.append(self.maxkwcode)
                    self.insertcombfreq( (self.maxkwcode,),1 )
                    self.maxkwcode+=1
                else:
                    itm=self.kwcode[kw]
                    self.insertcombfreq( (itm,),1 )
                    inputkws.append(itm)

            if len(inputkws)>1:
                self.insertitemset(tuple(inputkws))

    def final(self):
        if not self.stats:
            yield ('itemset_id', 'itemset_length', 'itemset_frequency', 'item')
        else:
            yield ('MaxTransactionLength', 'CombinationCount', 'PassedTransactions', 'ValidKeywords')

        splist=[{},{}]
        del(self.kwcode)
        splist[1]=self.overthres

        if self.stats:
            yield [self.maxlength, len(splist[1]), len(self.input), len(self.passedkw)]

        if not self.stats:
            for its,v in sorted(splist[1].items(), key=itemgetter(1),reverse=True):
                self.itemset_id+=1
                for i in self.demultiplex( (self.itemset_id, len([self.codekw[i] for i in its]), v, [self.codekw[i] for i in its]) ):
                    yield i

        if self.maxlen==None:
            self.maxlen=self.maxlength
        for l in xrange(2, min(self.maxlength+1, self.maxlen+1)):
            splist.append({})
            self.belowthres={}
            self.overthres={}
            prevl=l-1

            # Autothresholding
            if self.autothres==1:
                if len(self.input)==0 or len(self.passedkw)==0:
                    break
                else:
                    self.threshold=self.startingthreshold + int(len(self.passedkw)/len(self.input))

            self.cleanitemsets(l)
            self.passedkw={}
            prevsplist = splist[prevl]
            icombs = itertools.combinations
            insertcomb = self.insertcombfreq

            for k,v in self.input.iteritems():
                for k in icombs(k,l):
                    insertit=True
                    for i1 in icombs(k, prevl):
                        if i1 not in prevsplist:
                            insertit=False
                            break

                    if insertit:
                        insertcomb( k,v )

            splist[l-1]={}
            splist[l]=self.overthres

            if self.stats:
                yield [self.maxlength, len(splist[l]), len(self.input), len(self.passedkw)]

            if not self.stats:
                for its,v in sorted(splist[l].items(), key=itemgetter(1),reverse=True):
                    self.itemset_id+=1
                    for i in self.demultiplex( (self.itemset_id, len([self.codekw[i] for i in its]), v, [self.codekw[i] for i in its]) ):
                        yield i

        del(self.overthres)
        del(self.belowthres)
        del(self.passedkw)
        del(self.input)
        del(self.codekw)
        del(splist)

class sampledistvals:
    """

    .. function:: sampledistvals(sample_size, C1, C2, C3) -> [C1, C2, C3]

    Sampledistvals returns sample_size distinct values for each of the input C1..Cn columns.

    >>> table1('''
    ... test1 2 3
    ... test1 2 3
    ... test2 4 2
    ... test4 2 t
    ... ''')
    >>> sql("select sampledistvals(3, a, b, c) from table1")
    C1                        | C2    | C3
    ---------------------------------------------
    ["test1","test2","test4"] | [2,4] | [2,3,"t"]
    """
    registered=True

    def __init__(self):
        self.vals=None
        self.lenargs = -1
        self.init=True

    def step(self, *args):
        if self.init:
            self.lenargs = len(args)
            self.vals = a=[set() for i in xrange(self.lenargs-1)]
            self.init = False

        for i in xrange(1, self.lenargs):
            if len(self.vals[i-1])<args[0] and args[i] not in self.vals[i-1]:
                self.vals[i-1].add(args[i])

    def final(self):
        yield tuple(['C'+str(i) for i in xrange(1, self.lenargs)] )
        yield [jopts.toj(list(i)) for i in self.vals]

class sample:
    """

    .. function:: sample(sample_size, C1, C2, C3)

    Sample returns a random sample_size set of rows.

    >>> table1('''
    ... test1 2 3
    ... test1 2 3
    ... test2 4 2
    ... test4 2 t
    ... ''')

    >>> sql("select sample(2, a, b, c) from table1") # doctest: +ELLIPSIS
    C1    | C2 | C3
    ---------------
    ...
    """
    registered=True

    def __init__(self):
        self.samplelist = []
        self.index = 0

    def step(self, *args):
        sample_count = args[0]

        # Generate the reservoir
        if self.index < sample_count:
            self.samplelist.append(args[1:])
        else:
            r = random.randint(0, self.index)
            if r < sample_count:
                self.samplelist[r] = args[1:]

        self.index += 1


    def final(self):
        if len(self.samplelist) == []:
            yield tuple(['C1'])
        else:
            yield tuple(['C'+str(i) for i in xrange(1, len(self.samplelist[0]) + 1)] )
            for r in self.samplelist:
                yield list(r)

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

