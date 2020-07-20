"""
.. function:: variables()

Returns the defined variables with their values.

:Returned table schema:
    - *variable* text
        Variable name.
    - *value* text
        Variable value

.. toadd See also variables..

Examples:

    >>> sql("var 'env' 'testing' ")
    var('env','testing')
    --------------------
    testing
    >>> sql("variables")
    variable | value
    -------------------
    flowname |
    execdb   | :memory:
    env      | testing


"""

import vtbase
import functions
registered=True

class Variables(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        yield [('variable', 'text'),('value', 'text')]

        for i in functions.variables.__dict__:
            yield [i,functions.variables.__dict__[i]]

def Source():
    return vtbase.VTGenerator(Variables)



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