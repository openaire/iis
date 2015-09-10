import functions
import operator

class partialsort:
    """

    .. function:: partialsort(n,col1,col2,col3,....)

    sorts the first n columns of its input

    :Returned multiset schema:
        Columns are automatically named as col1, col2 ...


    Examples:

    >>> table1('''
    ... aa  43
    ... ac  34
    ... ab  21
    ... as  23
    ... ''')
    >>> sql("select partialsort(1,a,b) from table1")
    c1 | c2
    -------
    aa | 43
    ab | 21
    ac | 34
    as | 23

    """
    registered=True

    def __init__(self):
        self.topn=[]
        self.lessval=None
        self.stepsnum=0
        self.sortnum =  None

    def step(self, *args):
        if len(args)<2:
            raise functions.OperatorError("partialsort","Wrong number of arguments")
        if not self.sortnum:
            self.sortnum = tuple(i for i in xrange(args[0]))
        self.topn.append(args[1:])
        self.stepsnum+=1
        pass

    def final(self):
        yield tuple('c'+str(i) for i in xrange(1,len(self.topn[0])+1))
        self.topn.sort(key=operator.itemgetter(*self.sortnum))
        for el in self.topn:
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

