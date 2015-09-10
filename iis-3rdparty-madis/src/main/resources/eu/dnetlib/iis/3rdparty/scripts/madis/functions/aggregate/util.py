__docformat__ = 'reStructuredText en'

class expandgroups:
    """
    .. function:: expandgroups(args) -> args

    Expands the contents of groups. Usefull for debugging group related queries.

    Examples:

    Directed graph:

    >>> table1('''
    ... 1   2
    ... 2   3
    ... 1   4
    ... 2   5
    ... 3   3
    ... ''')

    >>> sql("select expandgroups(a,b) from table1")
    C1 | C2
    -------
    1  | 2
    2  | 3
    1  | 4
    2  | 5
    3  | 3

    >>> sql("select expandgroups(a,b) as gr from table1")
    gr1 | gr2
    ---------
    1   | 2
    2   | 3
    1   | 4
    2   | 5
    3   | 3

    >>> sql("select a,expandgroups(b) as gr from table1 group by a")
    a | gr
    ------
    1 | 2
    1 | 4
    2 | 3
    2 | 5
    3 | 3

    """

    registered=True

    def __init__(self):
        self.rows=[]

    def step(self, *args):
        self.rows.append(args)

    def final(self):
        yield tuple(('C'+str(x) for x in xrange(1,len(self.rows[0])+1)))
        for r in self.rows:
            yield r

class showgroups:
    """
    .. function:: showgroups(args) -> string

    Shows the contents of groups. Usefull for debugging group related queries.

    Examples:

    Directed graph:

    >>> table1('''
    ... 1   2
    ... 2   3
    ... 1   4
    ... 2   5
    ... 3   3
    ... ''')

    >>> sql("select showgroups(a,b) from table1") # doctest: +NORMALIZE_WHITESPACE
    showgroups(a,b)
    --------------------
    <BLANKLINE>
    1        2
    2        3
    1        4
    2        5
    3        3


    >>> sql("select showgroups(b) as gr from table1 group by a")
    gr
    ----
    <BLANKLINE>
    2
    4
    <BLANKLINE>
    3
    5
    <BLANKLINE>
    3

    """

    registered=True

    def __init__(self):
        self.rows=[]

    def step(self, *args):
        self.rows.append(args)

    def final(self):
        return '\n'+'\n'.join(['\t'.join([unicode(x) for x in r]) for r in self.rows])

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
