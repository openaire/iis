__docformat__ = 'reStructuredText en'

import lib.jopts as jopts
import json

try:
    from collections import OrderedDict
except ImportError:
    # Python 2.6
    from lib.collections26 import OrderedDict


class jgroup:
    """
    .. function:: jgroup(columns)

    Groups columns of a group into a jpack.

    Example:

    >>> table1('''
    ... word1   1
    ... word2   1
    ... word3   2
    ... word4   2
    ... ''')
    >>> sql("select jgroup(a) from table1 group by b")
    jgroup(a)
    -----------------
    ["word1","word2"]
    ["word3","word4"]
    >>> sql("select jgroup(a,b) from table1")
    jgroup(a,b)
    -------------------------------------------------
    [["word1",1],["word2",1],["word3",2],["word4",2]]

    >>> table2('''
    ... [1,2]   1
    ... [3,4]   1
    ... [5,6]   2
    ... [7,8]   2
    ... ''')

    >>> sql("select jgroup(a) from table2")
    jgroup(a)
    -------------------------
    [[1,2],[3,4],[5,6],[7,8]]

    >>> sql("select jgroup(a,b) from table2")
    jgroup(a,b)
    -----------------------------------------
    [[[1,2],1],[[3,4],1],[[5,6],2],[[7,8],2]]

    >>> sql("select jgroup(jdict('a',a,'b',b)) from table2")
    jgroup(jdict('a',a,'b',b))
    -------------------------------------------------------------------------
    [{"a":[1,2],"b":1},{"a":[3,4],"b":1},{"a":[5,6],"b":2},{"a":[7,8],"b":2}]
    """

    registered = True #Value to define db operator

    def __init__(self):
        self.outgroup = []

    def step(self, *args):
        if len(args) == 1:
            self.outgroup += (jopts.elemfromj(args[0]))
        else:
            self.outgroup.append(jopts.elemfromj(*args))

    def final(self):
        return jopts.toj(self.outgroup)


class jgroupunion:
    """
    .. function:: jgroupunion(columns) -> jpack

    Calculates the union of the jpacks (by treating them as sets) inside a group.

    Example:

    >>> table1('''
    ... '[1,2]' 6
    ... '[2,3]' 7
    ... '[2,4]' '[8,11]'
    ... 5 9
    ... ''')
    >>> sql("select jgroupunion(a,b) from table1")
    jgroupunion(a,b)
    ----------------------
    [1,2,6,3,7,4,8,11,5,9]

    >>> sql("select jgroupunion(1)")
    jgroupunion(1)
    --------------
    1

    """

    registered = True #Value to define db operator

    def __init__(self):
        self.outgroup = OrderedDict()
        self.outgroupupdate = self.outgroup.update

    def step(self, *args):
        self.outgroupupdate([(x, None) for x in jopts.fromj(*args)])

    def final(self):
        return jopts.toj(list(self.outgroup))


class jgroupintersection:
    """
    .. function:: jgroupintersection(columns) -> jpack

    Calculates the intersection of all jpacks (by treating them as sets) inside a group.

    Example:

    >>> table1('''
    ... '[1,2]' 2
    ... '[2,3]' 2
    ... '[2,4]' '[2,11]'
    ... 2 2
    ... ''')
    >>> sql("select jgroupintersection(a,b) from table1")
    jgroupintersection(a,b)
    -----------------------
    2

    >>> sql("select jgroupintersection(1)")
    jgroupintersection(1)
    ---------------------
    1

    """

    registered = True #Value to define db operator

    def __init__(self):
        self.outgroup = None
        self.outset = None

    def step(self, *args):
        if self.outgroup == None:
            self.outgroup = OrderedDict([(x, None) for x in jopts.fromj(args[0])])
            self.outset = set(self.outgroup)
        for jp in args:
            for i in self.outset.difference(jopts.fromj(jp)):
                del (self.outgroup[i])
            self.outset = set(self.outgroup)

    def final(self):
        return jopts.toj(list(self.outgroup))


class jdictgroupunion:
    """
    .. function:: jgroupunion(jdicts) -> jdict

    Calculates the union of all jdicts inside a group. The returned jdict's key values, are
    calculated as the max length of the lists (or dictionaries) that have been found inside
    the individual jdicts of the group.

    Example:

    >>> table1('''
    ... '{"b":1, "a":1}'
    ... '{"c":1, "d":[1,2,3]}'
    ... '{"b":{"1":2,"3":4}, "d":1}'
    ... ''')
    >>> sql("select jdictgroupunion(a) from table1")
    jdictgroupunion(a)
    -------------------------
    {"b":2,"a":1,"c":1,"d":3}

    """

    registered = True #Value to define db operator

    def __init__(self):
        self.outgroup = OrderedDict()

    def step(self, *args):
        for d in args:
            for x, v in json.loads(d, object_pairs_hook=OrderedDict).iteritems():
                vlen = 1
                if type(v) in (list, OrderedDict):
                    vlen = len(v)
                try:
                    if vlen > self.outgroup[x]:
                        self.outgroup[x] = vlen
                except KeyError:
                    self.outgroup[x] = vlen

    def final(self):
        return json.dumps(self.outgroup, separators=(',', ':'), ensure_ascii=False)


class jgroupunionkeys:
    """
    .. function:: jgroupunionkeys(columns) -> jpack

    Calculates the union of the jdict keys. Use it with care, because for performance
    reasons the input data are not checked at all. They should all be jdicts.

    Example:

    >>> table1('''
    ... '{"1":1, "2":3}' '{"a":5}'
    ... '{"2":1, "3":3}' '{}'
    ... ''')
    >>> sql("select jgroupunionkeys(a,b) from table1")
    jgroupunionkeys(a,b)
    --------------------
    ["1","2","a","3"]

    >>> sql("select jgroupunionkeys('{}')")
    jgroupunionkeys('{}')
    ---------------------
    []
    """

    registered = True #Value to define db operator

    def __init__(self):
        self.outgroup = OrderedDict()
        self.outgroupset = set()

    def step(self, *args):
        for arg in args:
            v = json.loads(arg)
            if not set(v).issubset(self.outgroup):
                self.outgroupset.update(v)
                self.outgroup.update([(k, None) for k in json.loads(arg, object_pairs_hook=OrderedDict).iterkeys()])

    def final(self):
        return jopts.toj(list(self.outgroup))


class jgroupuniquelimit:
    """
    .. function:: jgroupuniquelimit(jpack, k, limit) -> jpack

    Returns the k where the unique values inside all jpacks have reached limit.

    Example:

    >>> table1('''
    ... '[1,2]' 1
    ... '[2,3,4,5]' 2
    ... '[2,4]' 3
    ... 5 4
    ... ''')
    >>> sql("select jgroupuniquelimit(a,b,3) from table1")
    jgroupuniquelimit(a,b,3)
    ------------------------
    2

    """

    registered = True #Value to define db operator

    def __init__(self):
        self.gset = set()
        self.k = None

    def step(self, *args):
        if self.k is None:
            self.gset.update([(x, None) for x in jopts.fromj(args[0])])

            if len(self.gset) >= args[-1]:
                self.k = args[1]

    def final(self):
        return self.k


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
