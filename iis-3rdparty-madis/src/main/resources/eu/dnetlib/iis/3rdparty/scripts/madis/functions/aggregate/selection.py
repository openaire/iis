import setpath
import Queue
import functions

__docformat__ = 'reStructuredText en'

class imax:
    """

    .. function:: imax(i,value)

    Returns the i-th max value of the group.
            
    Examples:        
    
    >>> table1('''
    ... 34  la
    ... 18   lo
    ... 120.0  all
    ... ''')
    >>> sql("select imax(1,a) as first from table1")
    first
    -----
    120
    >>> sql("select imax(3,a) as third from table1")
    third
    -----
    18
    >>> sql("select imax(2,cast( a as text))  as secstr from table1")
    secstr
    ------
    18
    >>> sql("select imax(4,a) from table1")
    imax(4,a)
    ---------
    None
    >>> sql("select imax(4,a) from (select 5 as a where a!=5)")
    imax(4,a)
    ---------
    None


    """
    registered=True

    def __init__(self):
        self.topn=None
        self.size=None
        self.strtype=False
        self.anytype=True
        self.lessval=None
        self.stepsnum=0
        self.valarg=None

    def step(self, *args):
        if not args:
            raise functions.OperatorError("imax","No arguments")
        if len(args)<2:
            raise functions.OperatorError("imax","Wrong number of arguments")
        if not self.size:
            try:
                self.size=int(args[0])
                self.topn=Queue.PriorityQueue(self.size)
            except ValueError:
                raise functions.OperatorError("imax","Wrong type in first argument")

        curval=args[1]
        if not self.topn.full():
            self.topn.put_nowait(curval)
        else:
            curless=self.topn.get()
            self.topn.put_nowait(max(curval,curless))
        self.stepsnum+=1


    def final(self):
        if not self.size:
            return
        if self.stepsnum<self.size:
                return None
        return self.topn.get()

class q2list:
    def __init__(self,queue):
        self.q=queue
    def __iter__(self):
        return self
    def next(self):
        if self.q.empty():
            raise StopIteration
        a=self.q.get_nowait()
        self.q.put_nowait(a)
        return a


def typed(arg):
    try:
        arg=int(arg)
    except ValueError:
        try:
            arg=float(arg)
        except ValueError:
            pass
    return

class minrow:
    """

    .. function:: minrow(compare,value)

    Compares group members over the first argument (i.e. *compare*).
    When the minimum is located, it returns the corresponding value in the second argument (i.e. *value*).

    Examples:

    >>> table1('''
    ... 34  la
    ... 18   lo
    ... 120.0  all
    ... ''')
    >>> sql("select minrow(a,b) as b from table1")
    b
    --
    lo
    >>> sql("select minrow(a,a) as a from table1")
    a
    --
    18

    .. doctest::
        :hide:

    >>> sql("select minrow(a,a) as a from (select 5 as a where a!=5)")
    a
    ----
    None
    
    """
    registered=True


    def __init__(self):
        self.minv=None
        
    def step(self, *args):
        if not args:
            raise functions.OperatorError("minrow","No arguments")
        if len(args)!=2:
            raise functions.OperatorError("minrow","Wrong number of arguments")
        if not self.minv:
            self.minv=(args[0],args[1])
        elif args[0]<self.minv[0]:
            self.minv=(args[0],args[1])


    def final(self):
        if not self.minv:
            return None
        return self.minv[1]

class maxrow:
    """
    .. function:: maxrow(compare,value)

    Compares group members over the first argument (i.e. *compare*).
    When the maximum is located, it returns the corresponding value in the second argument (i.e. *value*).

    Examples:

    >>> table1('''
    ... 34  la
    ... 18   lo
    ... 120.0  all
    ... ''')
    >>> sql("select maxrow(a,b) as b from table1")
    b
    ---
    all
    >>> sql("select maxrow(a,a) as a from table1")
    a
    ---
    120
    >>> sql("select maxrow(b,a) as a from table1")
    a
    --
    18
    """
    registered=True


    def __init__(self):
        self.maxv=None
        self.first=True

    def step(self, *args):
        if self.first:
            if not args:
                raise functions.OperatorError("maxrow","No arguments")
            if len(args)!=2:
                raise functions.OperatorError("maxrow","Wrong number of arguments")
            self.maxv=(args[0],args[1])
            self.first=False
            return
        self.maxv=max(self.maxv,args)


    def final(self):
        if not self.maxv:
            return None
        return self.maxv[1]

class groupdiff:
    """
    .. function:: groupdiff(compare,values)

    Compares group members over the first argument (i.e. *compare*). It keeps only the rows where *compare* changes.
    It appends a column at the end containing how many times *compare* repeats.

    Examples:

    >>> table1('''
    ... 0  a
    ... 0  b
    ... 1  c
    ... 1  d
    ... 2  e
    ... 3  e
    ... 3  f
    ... 3  g
    ... ''')
    >>> sql("select groupdiff(a,b) as b from table1")
    b1 | b2 | b3
    ------------
    0  | a  | 2
    1  | c  | 2
    2  | e  | 1
    3  | e  | 3
    >>> sql("select groupdiff(a) as a from table1")
    a1 | a2
    -------
    0  | 2
    1  | 2
    2  | 1
    3  | 3
    >>> sql("select groupdiff(b,a) as a from table1")
    a1 | a2 | a3
    ------------
    a  | 0  | 1
    b  | 0  | 1
    c  | 1  | 1
    d  | 1  | 1
    e  | 2  | 2
    f  | 3  | 1
    g  | 3  | 1
    """
    registered=True

    def __init__(self):
        self.first=True
        self.data=[]
        self.prevcomp=None
        self.size=0
        self.repeat=1

    def step(self, *args):
        if self.first:
            if not args:
                raise functions.OperatorError("groupdiff","No arguments")
            self.prevcomp=args[0]
            self.data.append(list(args))
            self.first=False
            self.size=len(args)
            return

        if args[0]!=self.prevcomp:
            self.prevcomp=args[0]
            self.data[-1].append(self.repeat)
            self.data.append(list(args))
            self.repeat=1
        else:
            self.repeat+=1

    def final(self):
        self.data[-1].append(self.repeat)
        yield tuple(["compid"]+["C"+str(i) for i in xrange(1,self.size)]+["repetition"])
        for i in self.data:
            yield i


class ontop:
    """

    .. function:: ontop(n,compare,value1,value2,....) -> [colname1, colname2 ...]

    Compares group members over the second argument (i.e. *compare*), so as to locate the top *n* members
    (specified in the first argument) and then returns the corresponding data under the specified columns
    *value1, value2, ....*.
    
    :Returned multiset schema:
        Columns are automatically named as *colname1 text, colname2 text...*

    .. seealso::
    
       * :ref:`tutmultiset` functions

   
    Examples:
      
    >>> table1('''
    ... 34  la
    ... 18   lo
    ... 120.0  all
    ... ''')
    >>> sql("select ontop(1,a,b) from table1")
    top1
    ----
    all
    >>> sql("select ontop(2,a,a,b) from table1")
    top1 | top2
    -----------
    120  | all
    34   | la
    
    >>> sql("select ontop(2,a,a,b,a,b) from table1")
    top1 | top2 | top3 | top4
    -------------------------
    120  | all  | 120  | all
    34   | la   | 34   | la

    >>> sql("select ontop(pk) from (select 5 as pk where pk!=5)")
    top1
    -
    """
    registered=True
    multiset=True


    def __init__(self):
        self.topn=None
        self.size=None
        self.lessval=None
        self.stepsnum=0
        self.argnum = 1

    def step(self, *args):
        if not args:
            raise functions.OperatorError("ontop","No arguments")
        if len(args)<3:
            raise functions.OperatorError("ontop","Wrong number of arguments")
        if not self.size:
            try:
                self.size=int(args[0])
                self.topn=Queue.PriorityQueue(self.size)
                self.argnum = len(args)-2
            except ValueError:
                raise functions.OperatorError("ontop","Wrong type in first argument")

        inparg=args[1]
        outarg=args[2:]

        if not self.topn.full():
            self.topn.put_nowait((inparg,outarg))       
        else:
            inparg_old , outarg_old=self.topn.get_nowait()     
            self.topn.put_nowait(max((inparg,outarg),(inparg_old ,outarg_old)))

        self.stepsnum+=1


    def final(self):
        output=[]
        if self.topn:
            while not self.topn.empty():
                output+=[self.topn.get_nowait()[1]]

        yield tuple(["top"+str(i+1) for i in xrange(self.argnum)])

        for el in reversed(output):
            yield el


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
        
