import itertools

class peekable():
    """ An iterator that supports a peek operation.  Example usage:
    >>> p = peekable(range(4))
    >>> p.peek( )
    0
    >>> p.next()
    0
    >>> p.peek()
    1
    >>> p.next()
    1
    >>> p.peek()
    2
    >>> p.peek()
    2
    >>> p.next()
    2
    >>> p.peek()
    3
    >>> p.next()
    3
    >>> p.peek()
    Traceback (most recent call last):
    ...
    StopIteration
    """
    
    def __init__(self, iterable):
        self._srciter = iter(iterable)
        self._iter = self._srciter
        self.next=self._iter.next

    def __iter__(self):
        return self

    def peek(self):
        tmp=self._iter.next()
        self._iter=itertools.chain([tmp], self._srciter)
        self.next=self._iter.next
        return tmp

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    if __name__ == "__main__":
        import doctest
        doctest.testmod()