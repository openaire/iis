from . import setpath
from lib import jopts
from lib.jsonpath import jsonpath as libjsonpath
import json
import operator
import itertools
import re
import functions
import math
try:
    from collections import OrderedDict
except ImportError:
    # Python 2.6
    from lib.collections26 import OrderedDict


def jpack(*args):

    """
    .. function:: jpack(args...) -> jpack

    Converts multiple input arguments into a single string. Jpacks preserve the types
    of their inputs and are based on JSON encoding. Single values are represented as
    themselves where possible.

    Examples:

    >>> sql("select jpack('a')")
    jpack('a')
    ----------
    a

    >>> sql("select jpack('a','b',3)")
    jpack('a','b',3)
    ----------------
    ["a","b",3]

    >>> sql("select jpack('a', jpack('b',3))")
    jpack('a', jpack('b',3))
    ------------------------
    ["a",["b",3]]

    """

    return jopts.toj(jopts.elemfromj(*args))

jpack.registered=True

def jngrams(*args):

    """
    .. function:: jngrams(n,text) -> jpack

    Converts multiple input arguments into a jpack of ngrams.

    Examples:

    >>> sql("select jngrams(1,'This is a test phrase')")
    jngrams(1,'This is a test phrase')
    -------------------------------------------
    [["This"],["is"],["a"],["test"],["phrase"]]

    >>> sql("select jngrams(2,'This is a test phrase')")
    jngrams(2,'This is a test phrase')
    ---------------------------------------------------------
    [["This","is"],["is","a"],["a","test"],["test","phrase"]]


    """
    if type(args[0]) == int:
        n = args[0]
        text = args[1]
    else:
        n = 1
        text = args[0]
    g = text.split(' ')
    listofngrams = []
    for i in range(len(g)-n+1):
        listofngrams.append(g[i:i+n])
    return jopts.toj(listofngrams)

jngrams.registered=True

def jfrequentwords(*args):

    """
    .. function:: jfrequentwords(args...) -> jpack

    Returns the frequent words of a text in a jpack

    """
    wordslist = args[0].split(' ')
    setwords = set(wordslist)
    c = dict.fromkeys(setwords, 0)
    for w in wordslist:
        c[w]+=1
    lenwords = len(setwords)
    extremevals = int(math.ceil(lenwords * 3 * 1.0/100))
    frequences = sorted(c.values())[extremevals:(lenwords-extremevals)]
    avgfrequency = math.ceil(sum(frequences)*1.0/len(frequences))

    return jopts.toj([k for k,v in c.items() if v >= avgfrequency])

jfrequentwords.registered=True

def jsonstrict(*args):

    """
    .. function:: jsonstrict(args...) -> json string

    Sometimes we wish to process json lists from another application. Jsonstrict function
    tries to always create json compatible lists. So it always returns json lists.

    Examples:

    >>> sql("select jsonstrict('a')")
    jsonstrict('a')
    ---------------
    ["a"]

    >>> sql("select jsonstrict('a','b',3)")
    jsonstrict('a','b',3)
    ---------------------
    ["a","b",3]

    >>> sql("select jsonstrict('a', jpack('b',3))")
    jsonstrict('a', jpack('b',3))
    -----------------------------
    ["a",["b",3]]

    """

    return json.dumps(jopts.elemfromj(*args), separators=(',',':'), ensure_ascii=False)

jsonstrict.registered=True

def jzip(*args):

    """
    .. function:: jzip(args...) -> json string

    It combines the corresponding elements of input jpacks.

    Examples:

    >>> sql('''select jzip('["a", "b"]', '[1,2]','[4,5]')''')
    jzip('["a", "b"]', '[1,2]','[4,5]')
    -----------------------------------
    [["a",1,4],["b",2,5]]

    """
    return json.dumps([list(x) for x in zip(*jopts.elemfromj(*args))], separators=(',',':'), ensure_ascii=False)

jzip.registered=True

def jzipdict(*args):

    """
    .. function:: jzipdict(args...) -> json string

    It combines the correspinding elements of input jpacks into a jdict.

    Examples:

    >>> sql('''select jzipdict('["a", "b"]', '[1,2]','[4,5]')''')
    jzipdict('["a", "b"]', '[1,2]','[4,5]')
    ---------------------------------------
    {"a":[1,4],"b":[2,5]}

    """
    return json.dumps(dict(tuple([x[0], x[1:]]) for x in zip(*jopts.elemfromj(*args))), separators=(',',':'), ensure_ascii=False)

jzipdict.registered=True

def jlen(*args):

    """
    .. function:: jlen(args...) -> int

    Returns the total length in elements of the input jpacks.

    Examples:

    >>> sql("select jlen('abc')")
    jlen('abc')
    -----------
    1

    >>> sql("select jlen('a','b',3)")
    jlen('a','b',3)
    ---------------
    3

    >>> sql("select jlen('a', jpack('b',3))")
    jlen('a', jpack('b',3))
    -----------------------
    3

    >>> sql("select jlen('[1,2,3]')")
    jlen('[1,2,3]')
    ---------------
    3

    """
    return sum([len(x) if type(x) in (dict,list) else 1 for x in (jopts.elemfromj(*args))])

jlen.registered=True

def jrange(num):
    """
    .. function:: jrange(num) -> jrange

    Returns a jrange of integer numbers.

    Examples:

    >>> sql("select jrange(5)")
    jrange('a')
    -----------------
    ["0","1","2","3"]

    """
    jran = [None]*num
    for i in range(num):
        jran[i] = str(i)
    return jopts.toj(jran)

jrange.registered=True

def jfilterempty(*args):
    """
    .. function:: jfilterempty(jpacks.) -> jpack

    Removes from input jpacks all empty elements.

    Examples:

    >>> sql("select jfilterempty('a', '', '[]')")
    jfilterempty('a', '', '[]')
    ---------------------------
    a

    >>> sql("select jfilterempty('a','[null]',3)")
    jfilterempty('a','[null]',3)
    ----------------------------
    ["a",3]

    >>> sql("select jfilterempty('[3]', jpack('b', ''))")
    jfilterempty('[3]', jpack('b', ''))
    -----------------------------------
    [3,"b"]

    """

    return jopts.toj([x for x in jopts.fromj(*args) if x!='' and x!=[] and x!=None])

jfilterempty.registered=True

def jlengthiest(*args):
    """
    .. function:: jlengthiest(jpacks.) -> jpack

    Returns the string with the greatest length contained in the jpacks.

    Examples:

    >>> sql("select jlengthiest('a', '', '[]')")
    jlengthiest('a', '', '[]')
    --------------------------
    a

    >>> sql("select jlengthiest('a','longer',3)")
    jlengthiest('a','longer',3)
    ---------------------------
    longer

    >>> sql("select jlengthiest('[3]', jpack('b', ''))")
    jlengthiest('[3]', jpack('b', ''))
    ----------------------------------
    3

    """

    maxlen=-1
    res=None
    
    for i in (x for x in jopts.fromj(*args)):
        if i == None: 
            l=-1
        else:
            l = len(str(i))
        if l > maxlen:
            maxlen = l
            res = i

    return res

jlengthiest.registered=True

def jchars(*args):
    """
    .. function:: jletters(text) -> character jpack

    Splits an input text into its composing characters.

    Examples:

    >>> sql("select jchars('this is a text')")
    jchars('this is a text')
    ---------------------------------------------------------
    ["t","h","i","s"," ","i","s"," ","a"," ","t","e","x","t"]

    >>> sql("select jchars('another', 'text')")
    jchars('another', 'text')
    ---------------------------------------------
    ["a","n","o","t","h","e","r","t","e","x","t"]
    """

    output = []

    for i in args:
        output+=list(i)

    return json.dumps(output, separators=(',',':'), ensure_ascii=False)

jchars.registered=True

def j2s(*args):

    """
    .. function:: j2s(jpack) -> space separated string

    Converts multiple input jpacks to a space separated string. Newlines are converted to spaces.

    Examples:

    >>> sql("select j2s('[1,2,3]')") # doctest: +NORMALIZE_WHITESPACE
    j2s('[1,2,3]')
    --------------
    1 2 3

    >>> sql("select j2s('[1,2,3]','a')") # doctest: +NORMALIZE_WHITESPACE
    j2s('[1,2,3]','a')
    ------------------
    1 2 3 a

    >>> sql("select j2s('a', 'b')") # doctest: +NORMALIZE_WHITESPACE
    j2s('a', 'b')
    -------------
    a b

    """

    return ' '.join([ str(x).replace('\n',' ') for x in jopts.fromj(*args) ])

j2s.registered=True

def j2t(*args):

    """
    .. function:: j2t(jpack) -> tabpack

    Converts multiple input jpacks to a tab separated pack (tab separated values). If tab or newline characters are found in
    the source jpack they are converted to spaces.

    Examples:

    >>> sql("select j2t('[1,2,3]')") # doctest: +NORMALIZE_WHITESPACE
    j2t('[1,2,3]')
    --------------
    1        2        3

    >>> sql("select j2t('[1,2,3]','a')") # doctest: +NORMALIZE_WHITESPACE
    j2t('[1,2,3]','a')
    ------------------
    1        2        3        a

    >>> sql("select j2t('a', 'b')") # doctest: +NORMALIZE_WHITESPACE
    j2t('a', 'b')
    -------------
    a        b

    """

    return '\t'.join([ str(x).replace('\t', '    ').replace('\n',' ') for x in jopts.fromj(*args) ])

j2t.registered=True

def t2j(*args):

    """
    .. function:: t2j(tabpack) -> jpack

    Converts a tab separated pack to a jpack.

    Examples:

    >>> sql("select t2j(j2t('[1,2,3]'))") # doctest: +NORMALIZE_WHITESPACE
    t2j(j2t('[1,2,3]'))
    -------------------
    ["1","2","3"]

    >>> sql("select t2j('asdfasdf')") # doctest: +NORMALIZE_WHITESPACE
    t2j('asdfasdf')
    ---------------
    ["asdfasdf"]

    """
    
    fj=[]
    for t in args:
        fj+=t.split('\t')

    return json.dumps(fj, separators=(',',':'), ensure_ascii=False)

t2j.registered=True

def s2j(*args):

    """
    .. function:: s2j(tabpack) -> jpack

    Converts a space separated pack to a jpack.

    Examples:

    >>> sql("select s2j('1  2 3 ')") # doctest: +NORMALIZE_WHITESPACE
    s2j('1  2 3 ')
    --------------
    ["1","2","3"]
    """

    fj=[]
    for t in args:
        fj+=[x for x in t.split(' ') if x!='']

    return jopts.toj(fj)

s2j.registered=True

def nl2j(*args):

    """
    .. function:: nl2j(text) -> jpack

    Converts a text with newlines to a jpack.
    """

    fj=[]
    for t in args:
        fj+=[x for x in t.split('\n')]

    return jopts.toj(fj)

nl2j.registered=True

def j2nl(*args):

    """
    .. function:: j2nl(jpack) -> text

    Converts multiple input jpacks to a newline separated text.

    Examples:

    >>> sql("select j2nl('[1,2,3]')") # doctest: +NORMALIZE_WHITESPACE
    j2nl('[1,2,3]')
    ---------------
    1
    2
    3

    >>> sql("select j2nl('[1,2,3]','a')") # doctest: +NORMALIZE_WHITESPACE
    j2nl('[1,2,3]','a')
    -------------------
    1
    2
    3
    a

    >>> sql("select j2nl('a', 'b')") # doctest: +NORMALIZE_WHITESPACE
    j2nl('a', 'b')
    --------------
    a
    b

    """

    return '\n'.join([str(x) for x in jopts.fromj(*args)])

j2nl.registered = True

def jmerge(*args):

    """
    .. function:: jmerge(jpacks) -> jpack

    Merges multiple jpacks into one jpack.

    Examples:

    >>> sql("select jmerge('[1,2,3]', '[1,2,3]', 'a', 3 )") # doctest: +NORMALIZE_WHITESPACE
    jmerge('[1,2,3]', '[1,2,3]', 'a', 3 )
    -------------------------------------
    [1,2,3,1,2,3,"a",3]

    """

    return jopts.toj( jopts.fromj(*args) )

jmerge.registered=True


def jset(*args):
    """
    .. function:: jset(jpacks) -> jpack

    Returns a set representation of a jpack, unifying duplicate items.

    Examples:

    >>> sql("select jset('[1,2,3]', '[1,2,3]', 'b', 'a', 3 )") # doctest: +NORMALIZE_WHITESPACE
    jset('[1,2,3]', '[1,2,3]', 'b', 'a', 3 )
    ----------------------------------------
    [1,2,3,"a","b"]

    """

    return jopts.toj(sorted(set(jopts.fromj(*args))))

jset.registered = True


def jexcept(*args):
    """
    .. function:: jexcept(jpackA, jpackB) -> jpack

    Returns the items of jpackA except the items that appear on jpackB.

    Examples:

    >>> sql("select jexcept('[1,2,3]', '[1,2,3]')") # doctest: +NORMALIZE_WHITESPACE
    jexcept('[1,2,3]', '[1,2,3]')
    -----------------------------
    []

    >>> sql("select jexcept('[1,2,3]', '[1,3]')") # doctest: +NORMALIZE_WHITESPACE
    jexcept('[1,2,3]', '[1,3]')
    ---------------------------
    2

    """

    if len(args) < 2:
        raise functions.OperatorError("jexcept","operator needs at least two inputs")

    b = set(jopts.fromj(args[1]))
    return jopts.toj([x for x in jopts.fromj(args[0]) if x not in b])

jexcept.registered = True


def jintersection(*args):
    """
    .. function:: jintersection(jpackA, jpackB) -> jpack

    Returns the items of jpackA except the items that appear on jpackB.

    Examples:

    >>> sql("select jintersection('[1,2,3]', '[1,2,3]')") # doctest: +NORMALIZE_WHITESPACE
    jintersection('[1,2,3]', '[1,2,3]')
    -----------------------------------
    [1,2,3]

    >>> sql("select jintersection('[1,2,3]', '[1,3]', 1)") # doctest: +NORMALIZE_WHITESPACE
    jintersection('[1,2,3]', '[1,3]', 1)
    ------------------------------------
    1

    """

    if len(args) < 2:
        raise functions.OperatorError("jintersection","operator needs at least two inputs")

    return jopts.toj(sorted(set.intersection(*[set(jopts.fromj(x)) for x in args])))

jintersection.registered = True


def jsort(*args):

    """
    .. function:: jsort(jpacks) -> jpack

    Sorts the input jpacks.

    Examples:

    >>> sql("select jsort('[1,2,3]', '[1,2,3]', 'b', 'a', 3 )") # doctest: +NORMALIZE_WHITESPACE
    jsort('[1,2,3]', '[1,2,3]', 'b', 'a', 3 )
    -----------------------------------------
    [1,1,2,2,3,3,3,"a","b"]

    """

    return jopts.toj(sorted( jopts.fromj(*args) ))

jsort.registered=True

def jsplitv(*args):

    """
    .. function:: jsplitv(jpacks) -> [C1]

    Splits vertically a jpack.

    Examples:

    >>> sql("select jsplitv(jmerge('[1,2,3]', '[1,2,3]', 'b', 'a', 3 ))") # doctest: +NORMALIZE_WHITESPACE
    C1
    --
    1
    2
    3
    1
    2
    3
    b
    a
    3

    """

    yield ('C1', )

    for j1 in jopts.fromj(*args):
        yield [jopts.toj(j1)]

jsplitv.registered=True

def jsplit(*args):

    """
    .. function:: jsplit(jpacks) -> [C1, C2, ...]

    Splits horizontally a jpack.

    Examples:

    >>> sql("select jsplit('[1,2,3]', '[3,4,5]')") # doctest: +NORMALIZE_WHITESPACE
    C1 | C2 | C3 | C4 | C5 | C6
    ---------------------------
    1  | 2  | 3  | 3  | 4  | 5

    """

    fj=[jopts.toj(x) for x in jopts.fromj(*args)]

    if fj==[]:
        yield ('C1',)
            
    yield tuple( ['C'+str(x) for x in range(1,len(fj)+1)] )
    yield fj

jsplit.registered=True

def jflatten(*args):

    """
    .. function:: jflattten(jpacks) -> jpack

    Flattens all nested sub-jpacks.

    Examples:

    >>> sql(''' select jflatten('1', '[2]') ''') # doctest: +NORMALIZE_WHITESPACE
    jflatten('1', '[2]')
    --------------------
    ["1",2]

    >>> sql(''' select jflatten('[["word1", 1], ["word2", 1], [["word3", 2], ["word4", 2]], 3]') ''') # doctest: +NORMALIZE_WHITESPACE
    jflatten('[["word1", 1], ["word2", 1], [["word3", 2], ["word4", 2]], 3]')
    -------------------------------------------------------------------------
    ["word1",1,"word2",1,"word3",2,"word4",2,3]

    """

    return jopts.toj( jopts.flatten( jopts.elemfromj(*args) ))

jflatten.registered=True

def jmergeregexp(*args):

    """
    .. function:: jmergeregexp(jpacks) -> jpack

    Creates a regular expression that matches all of the jpack's contents. If the input
    jpack contains keyword pairs, then jmergeregexp returns a regular expression
    with named groups.

    Examples:

    >>> sql(''' select jmergeregexp('["abc", "def"]') ''') # doctest: +NORMALIZE_WHITESPACE
    jmergeregexp('["abc", "def"]')
    ------------------------------
    (?:abc)|(?:def)

    >>> sql(''' select jmergeregexp('[["pos", "p1"], ["neg", "n1"], ["pos", "p2"]]') ''') # doctest: +NORMALIZE_WHITESPACE
    jmergeregexp('[["pos", "p1"], ["neg", "n1"], ["pos", "p2"]]')
    -------------------------------------------------------------
    (?P<neg>n1)|(?P<pos>p1|p2)

    >>> sql(''' select jmergeregexp('[]') ''') # doctest: +NORMALIZE_WHITESPACE
    jmergeregexp('[]')
    ------------------
    _^


    >>> sql(''' select jmergeregexp('["ab",""]') ''') # doctest: +NORMALIZE_WHITESPACE
    jmergeregexp('["ab",""]')
    -------------------------
    (?:ab)

    """

    inp = jopts.fromj(*args)

    if len(inp)>0 and type(inp[0]) == list:
        out={}
        for x,y in inp:
            if x not in out:
                out[x] = [y]
            else:
                out[x].append(y)

        res = '|'.join('(?P<'+ x + '>' + '|'.join(y)+')' for x, y in out.items() if y!='')
        if res == '':
            res = '_^'
        return res

    res = '|'.join('(?:'+x+')' for x in inp if x!='')
    if res == '':
        res = '_^'
    return res

jmergeregexp.registered=True

def jmergeregexpnamed(*args):

    """
    .. function:: jmergeregexpnamed(jpacks) -> jpack

    Creates a regular expression that matches all of the jpack's contents with named groups. If the number of
    named groups in a regular expression is greater than 99, then the output will be a jpack of regular expressions.

    Examples:

    >>> sql(''' select jmergeregexpnamed('["abc", "def"]') ''') # doctest: +NORMALIZE_WHITESPACE
    jmergeregexpnamed('["abc", "def"]')
    -----------------------------------
    (abc)|(def)

    """

    inp = jopts.fromj(*args)
    inp.sort()

    out = []
    for g in range(0, len(inp), 99):
        out.append('|'.join('('+x+')' for x in inp[g:g+99]))

    return jopts.toj(out)

jmergeregexpnamed.registered=True

def jdict(*args):

    """
    .. function:: jdict(key, value, key1, value1) -> jdict

    Returns a jdict of the keys and value pairs.

    Examples:

    >>> sql(''' select jdict('key1', 'val1', 'key2', 'val2') ''') # doctest: +NORMALIZE_WHITESPACE
    jdict('key1', 'val1', 'key2', 'val2')
    -------------------------------------
    {"key1":"val1","key2":"val2"}

    >>> sql(''' select jdict('key', '{"k1":1,"k2":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdict('key', '{"k1":1,"k2":2}')
    -------------------------------
    {"key":{"k1":1,"k2":2}}

    >>> sql(''' select jdict('key', '["val1", "val2"]') ''') # doctest: +NORMALIZE_WHITESPACE
    jdict('key', '["val1", "val2"]')
    --------------------------------
    {"key":["val1","val2"]}

    >>> sql(''' select jdict('1') ''') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    OperatorError: Madis SQLError:
    Operator JDICT: At least two arguments required

    """

    if len(args)==1:
        raise functions.OperatorError('jdict',"At least two arguments required")

    result = OrderedDict()
    
    for i in range(0, len(args), 2):
        result[args[i]] = jopts.fromjsingle(args[i+1])

    return jopts.toj( result )

jdict.registered=True

def jdictkeys(*args):

    """
    .. function:: jdictkeys(jdict) -> jpack

    Returns a jpack of the keys of input jdict

    Examples:

    >>> sql(''' select jdictkeys('{"k1":1,"k2":2}', '{"k1":1,"k3":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictkeys('{"k1":1,"k2":2}', '{"k1":1,"k3":2}')
    -----------------------------------------------
    ["k1","k2","k3"]

    >>> sql(''' select jdictkeys('{"k1":1,"k2":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictkeys('{"k1":1,"k2":2}')
    ----------------------------
    ["k1","k2"]
    >>> sql(''' select jdictkeys('test') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictkeys('test')
    -----------------
    []
    >>> sql(''' select jdictkeys(1) ''') # doctest: +NORMALIZE_WHITESPACE
    jdictkeys(1)
    ------------
    []

    """
    
    if len(args)==1:
        keys=[]
        i=args[0]
        try:
            if i[0]=='{' and i[-1]=='}':
                keys=[x for x in json.loads(i, object_pairs_hook=OrderedDict).keys()]
        except TypeError as e:
            pass
    else:
        keys=OrderedDict()
        for i in args:
            try:
                if i[0]=='{' and i[-1]=='}':
                    keys.update([(x,None) for x in json.loads(i, object_pairs_hook=OrderedDict).keys()])
            except TypeError as e:
                pass
        keys=list(keys)
    return jopts.toj( keys )

jdictkeys.registered=True

def jdictvals(*args):

    """
    .. function:: jdictvals(jdict, [key1, key2,..]) -> jpack

    If only the first argument (jdict) is provided, it returns a jpack of the values of input jdict (sorted by the jdict keys).

    If key values are also provided, it returns only the keys that have been provided.

    Examples:

    >>> sql(''' select jdictvals('{"k1":1,"k2":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictvals('{"k1":1,"k2":2}')
    ----------------------------
    [1,2]

    >>> sql(''' select jdictvals('{"k1":1,"k2":2, "k3":3}', 'k3', 'k1', 'k4') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictvals('{"k1":1,"k2":2, "k3":3}', 'k3', 'k1', 'k4')
    ------------------------------------------------------
    [3,1,null]
    >>> sql(''' select jdictvals('{"k1":1}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictvals('{"k1":1}')
    ---------------------
    1
    >>> sql(''' select jdictvals('{"k1":1}') ''') # doctest: +NORMALIZE_WHITESPACE
    jdictvals('{"k1":1}')
    ---------------------
    1
    >>> sql(''' select jdictvals(1) ''') # doctest: +NORMALIZE_WHITESPACE
    jdictvals(1)
    ------------
    1

    """

    if type(args[0]) in (int,float) or args[0][0]!='{' or args[0][-1]!='}':
        return args[0]
    d=json.loads(args[0])
    if len(args)==1:
        d=list(d.items())
        d.sort(key=operator.itemgetter(1,0))
        vals=[x[1] for x in d]
    else:
        vals=[]
        for i in args[1:]:
            try:
                vals.append(d[i])
            except KeyboardInterrupt:
                raise
            except:
                vals.append(None)
        
    return jopts.toj( vals )

jdictvals.registered=True

def jdictsplit(*args):

    """
    .. function:: jdictsplit(jdict, [key1, key2,..]) -> columns

    If only the first argument (jdict) is provided, it returns a row containing the values of input jdict (sorted by the jdict keys).

    If key values are also provided, it returns only the columns of which the keys have been provided.

    Examples:

    >>> sql(''' select jdictsplit('{"k1":1,"k2":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    k1 | k2
    -------
    1  | 2

    >>> sql(''' select jdictsplit('{"k1":1,"k2":2, "k3":3}', 'k3', 'k1', 'k4') ''') # doctest: +NORMALIZE_WHITESPACE
    k3 | k1 | k4
    --------------
    3  | 1  | None

    """

    d=json.loads(args[0])
    if len(args)==1:
        d=sorted(d.items())
        yield tuple([x[0] for x in d])
        yield [jopts.toj(x[1]) for x in d]
    else:
        vals=[]
        yield tuple(args[1:])
        for i in args[1:]:
            try:
                vals.append(jopts.toj(d[i]))
            except KeyboardInterrupt:
                raise    
            except:
                vals.append(None)
        yield vals

jdictsplit.registered=True


def jdictsplitv(*args):

    """
    .. function:: jdictsplitv(jdict, [key1, key2,..]) -> columns

    If only the first argument (jdict) is provided, it returns rows containing the values of input jdict.

    If key values are also provided, it returns only the columns of which the keys have been provided.

    Examples:

    >>> sql(''' select jdictsplitv('{"k1":1,"k2":2}') ''') # doctest: +NORMALIZE_WHITESPACE
    key | val
    ---------
    k1  | 1
    k2  | 2

    >>> sql(''' select jdictsplitv('{"k1":1,"k2":2, "k3":3}', 'k3', 'k1', 'k4') ''') # doctest: +NORMALIZE_WHITESPACE
    key | val
    ---------
    k3  | 3
    k1  | 1

    """

    yield ('key', 'val')
    if len(args) == 1:
        dlist = json.loads(args[0], object_pairs_hook=OrderedDict)
        for k, v in dlist.items():
            yield [k, jopts.toj(v)]
    else:
        dlist = json.loads(args[0])
        for k in args[1:]:
            try:
                yield k, jopts.toj(dlist[k])
            except KeyError:
                pass

jdictsplitv.registered = True

def jdictgroupkey(*args):
    """
    .. function:: jdictgroupkey(list_of_jdicts, groupkey1, groupkey2, ...)

    It groups an array of jdicts into a hierarchical structure. The grouping is done
    first by groupkey1 then by groupkey2 and so on.

    If no groupkeys are provided, then the first key of array's first jdict is used as a groupkey.

    Examples:

    >>> sql('''select jdictgroupkey('[{"gkey":"v1", "b":1},{"gkey":"v1","b":2},{"gkey":"v2","b":1, "c":2}]') as j''')
    j
    ---------------------------------------------
    {"v1":[{"b":1},{"b":2}],"v2":[{"b":1,"c":2}]}

    >>> sql('''select jdictgroupkey('[{"gkey":"v1", "b":1},{"gkey":"v1","b":2},{"gkey":"v2","b":1, "c":2}]', "gkey") as j''')
    j
    ---------------------------------------------
    {"v1":[{"b":1},{"b":2}],"v2":[{"b":1,"c":2}]}

    >>> sql('''select jdictgroupkey('[{"gkey":"v1", "gkey2":"f1", "b":1},{"gkey":"v1", "gkey2":"f2", "b":2},{"gkey":"v1", "gkey2":"f2", "b":1, "c":2}]', "gkey", "gkey2") as j''')
    j
    --------------------------------------------------------------
    {"v1":{"gkey2":{"f1":[{"b":1}],"f2":[{"b":2},{"b":1,"c":2}]}}}

    """

    def recgroupkey(jdict, gkeys):
        outdict=OrderedDict()

        for d in jdict:
            if d[gkeys[0]] not in outdict:
                outdict[d[gkeys[0]]] = [d]
            else:
                outdict[d[gkeys[0]]].append(d)
            del(d[gkeys[0]])

        if len(gkeys)>1:
            outdict = OrderedDict([(x, recgroupkey(y, gkeys[1:])) for x,y in outdict.items()])

        return {gkeys[0]:outdict}

    outdict=OrderedDict()
    dlist=json.loads(args[0], object_pairs_hook=OrderedDict)

    if len(args) == 1:
        groupkeys = [next(iter(dlist[0]))]
    else:
        groupkeys = args[1:]

    outdict = recgroupkey(dlist, groupkeys)

    return jopts.toj(outdict[groupkeys[0]])

jdictgroupkey.registered=True

def jsplice(*args):

    """
    .. function:: jsplice(jpack, range1_start, range1_end, ...) -> jpack

    Splices input jpack. If only a single range argument is provided, it returns input jpack's element in provided position. If defined position
    index is positive, then it starts counting from the beginning of input jpack. If defined position is negative it starts counting from the
    end of input jpack.

    If more than one range arguments are provided, then the arguments are assumed to be provided in pairs (start, end) that define ranges inside
    the input jpack that should be joined together in output jpack.

    Examples:

    >>> sql(''' select jsplice('[1,2,3,4,5]',0) ''') # doctest: +NORMALIZE_WHITESPACE
    jsplice('[1,2,3,4,5]',0)
    ------------------------
    1

    >>> sql(''' select jsplice('[1,2,3,4,5]',-1) ''') # doctest: +NORMALIZE_WHITESPACE
    jsplice('[1,2,3,4,5]',-1)
    -------------------------
    5

    >>> sql(''' select jsplice('[1,2,3,4,5]',10) ''') # doctest: +NORMALIZE_WHITESPACE
    jsplice('[1,2,3,4,5]',10)
    -------------------------
    None

    >>> sql(''' select jsplice('[1,2,3,4,5]', 0, 3, 0, 2) ''') # doctest: +NORMALIZE_WHITESPACE
    jsplice('[1,2,3,4,5]', 0, 3, 0, 2)
    ----------------------------------
    [1,2,3,1,2]

    >>> sql(''' select jsplice('[1,2,3,4,5]', 2, -1) ''') # doctest: +NORMALIZE_WHITESPACE
    jsplice('[1,2,3,4,5]', 2, -1)
    -----------------------------
    [3,4]

    """

    largs=len(args)
    if largs==1:
        return args[0]

    fj=jopts.fromj(args[0])

    if largs==2:
        try:
            return jopts.toj(fj[args[1]])
        except KeyboardInterrupt:
            raise
        except:
            return None

    outj=[]
    for i in range(1,largs,2):
        try:
            outj+=fj[args[i]:args[i+1]]
        except KeyboardInterrupt:
            raise           
        except:
            pass

    return jopts.toj(outj)
        

jsplice.registered=True

def jcombinations(*args):

    """
    .. function:: jcombinations(jpack, r) -> multiset

    Returns all length r combinations of jpack.

    Examples:

    >>> sql('''select jcombinations('["t1","t2","t3"]',2)''')
    C1 | C2
    -------
    t1 | t2
    t1 | t3
    t2 | t3

    >>> sql('''select jcombinations('["t1","t2",["t3","t4"]]',2)''')
    C1 | C2
    ----------------
    t1 | t2
    t1 | ["t3","t4"]
    t2 | ["t3","t4"]

    >>> sql('''select jcombinations(null,2)''')


    >>> sql('''select jcombinations('["t1","t2","t3","t4"]')''')
    C1
    --
    t1
    t2
    t3
    t4
    """

    r=1
    if len(args)==2:
        r=args[1]

    yield tuple(('C'+str(x) for x in range(1,r+1)))
    for p in itertools.combinations(jopts.fromj(args[0]), r):
        yield [jopts.toj(x) for x in p]

jcombinations.registered=True

def jpermutations(*args):

    """
    .. function:: jpermutations(jpack, r) -> multiset

    Returns all length r permutations of jpack.

    Examples:

    >>> sql('''select jpermutations('["t1","t2","t3"]',2)''')
    C1 | C2
    -------
    t1 | t2
    t1 | t3
    t2 | t1
    t2 | t3
    t3 | t1
    t3 | t2

    >>> sql('''select jpermutations('["t1","t2",["t3","t4"]]',2)''')
    C1          | C2
    -------------------------
    t1          | t2
    t1          | ["t3","t4"]
    t2          | t1
    t2          | ["t3","t4"]
    ["t3","t4"] | t1
    ["t3","t4"] | t2

    >>> sql('''select jpermutations(null,2)''')

    >>> sql('''select jpermutations('["t1","t2","t3","t4"]')''')
    C1
    --
    t1
    t2
    t3
    t4
    """

    r=1
    if len(args)==2:
        r=args[1]

    yield tuple(('C'+str(x) for x in range(1,r+1)))
    for p in itertools.permutations(jopts.fromj(args[0]), r):
        yield [jopts.toj(x) for x in p]

jpermutations.registered=True


def jsonpath(*args):

    """
    .. function:: jsonpath(JSON, jsonpathexpr1, jsonpathexpr2) -> multiset

    Uses jsonpath expressions to pick values from inside a JSON input. If the outputs of all JSONpath expressions
    have the same number of elements in them, it splits the output into multiple rows.

    .. note::

        For more on JSONpath see: http://goessner.net/articles/JsonPath/

    Examples:

    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2", "value":"v2"}]}', '$.d1') ''')
    C1
    -------------------------------------------------------
    [{"name":"n1","value":"v1"},{"name":"n2","value":"v2"}]

    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2", "value":"v2"}]}', '$.d1[*].name') ''')
    C1
    --
    n1
    n2

    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2", "value":"v2"}]}', '$.d1[*].name', '$.d1[*].value') ''')
    C1 | C2
    -------
    n1 | v1
    n2 | v2

    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2", "value":"v2"}]}', '$.d1[*].name', '$.d1[*].nonexisting') ''')
    C1 | C2
    ---------
    n1 | None
    n2 | None

    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2"}]}', '$.d1[*].name', '$.d1[*].value') ''')
    C1          | C2
    ----------------
    ["n1","n2"] | v1
    
    >>> sql('''select jsonpath('{"d1":[{"name":"n1", "value":"v1"}, {"name":"n2", "value":"v2"}]}', '$.nonexisting') ''')


    """

    j = json.loads(args[0])

    yield tuple( ('C'+str(x)for x in range( 1,len(args) ) )   )
    output=[libjsonpath(j, jp, use_eval=False) for jp in args[1:]]

    l=0
    lchanges=0
    for i in output:
        try:
            if len(i)!=l:
                l=len(i)
                lchanges+=1
        except TypeError:
            pass

    if l==0:
        return

    if lchanges>1:
        yield [jopts.toj(x) if type(x)!=bool else None for x in output]
    else:
        for i in range(l):
            yield [jopts.toj(x[i]) if type(x)!=bool else None for x in output]

jsonpath.registered=True


if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    from . import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()