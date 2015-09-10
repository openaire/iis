__docformat__ = 'reStructuredText en'


class concatgroup:
    """
    .. function:: concatgroup(X)
    
    Concatenates strings in a group/dataset X.

    Example:

    >>> table1('''
    ... word1   1
    ... word2   1
    ... word3   2
    ... word4   2
    ... ''')
    >>> sql("select concatgroup(a) from table1 group by b")
    concatgroup(a)
    --------------
    word1word2
    word3word4
    """

    registered=True #Value to define db operator

    def __init__(self):
        self.whole_string=[]

    def step(self, *args):
        self.whole_string.append(args[0])

    def final(self):
        return ''.join(self.whole_string)

class concatterms:
    """
    .. function:: concatterms(text1, text2,...)

    Concatenates strings in a group/dataset X, while keeping them disjoint, i.e. using the single space delimiter.

    Examples:
    
    >>> table1('''
    ... word1   1
    ... word2   1
    ... word3   2
    ... word4   2
    ... ''')
    >>> sql("select concatterms(a) from table1 group by b")
    concatterms(a)
    --------------
    word1 word2
    word3 word4
    """

    registered=True #Value to define db operator

    def __init__(self):
        self.whole_string=[]

    def step(self, *args):
        if len(args[0])!=0:
            self.whole_string.append(args[0])

    def final(self):
        return ' '.join(self.whole_string)

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
