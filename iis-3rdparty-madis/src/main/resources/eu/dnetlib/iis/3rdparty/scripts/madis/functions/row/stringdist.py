# coding: utf-8

import setpath
import lib.stringdists as sd

def levendist(*args):
    """
    .. function:: levendist(str1, str2)

    Returns *int* which is the levenshtein distance between str1 and str2

    Examples:

    >>> sql("select levendist('HURQBOHP','QKHOZ') ")
    levendist('HURQBOHP','QKHOZ')
    -----------------------------
    7
    """
    if len(args)!=2:
        raise functions.OperatorError("levendist","operator accepts two arguments")

    return sd.levenshtein_dist(args[0],args[1])

levendist.registered=True

def damlevendist(*args):
    """
    .. function:: damlevendist(str1, str2)

    Returns *int* which is the damerau-levenshtein distance between str1 and str2

    Examples:

    >>> sql("select damlevendist('HURQBOHP','QKHOZ') ")
    damlevendist('HURQBOHP','QKHOZ')
    --------------------------------
    6
    """
    if len(args)!=2:
        raise functions.OperatorError("damlevendist","operator accepts two arguments")

    return sd.dameraulevenshtein_dist(args[0],args[1])

damlevendist.registered=True

def quickstrdist(*args):
    """
    .. function:: damlevendist(str1, str2)

    Returns *int* which is a string distance between str1 and str2, based on Python's
    difflib library. It is a lot faster than levendist or damlevendist.

    Examples:

    >>> sql("select quickstrdist('HURQBOHP','QKHOZ') ")
    quickstrdist('HURQBOHP','QKHOZ')
    --------------------------------
    8
    """
    if len(args)!=2:
        raise functions.OperatorError("quickstrdist","operator accepts two arguments")

    return sd.quick_string_dist(args[0],args[1])

quickstrdist.registered=True

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