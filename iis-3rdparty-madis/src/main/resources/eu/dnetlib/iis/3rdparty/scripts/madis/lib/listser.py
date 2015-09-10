from types import IntType, TupleType, StringType, FloatType, LongType, ListType, DictType, NoneType, BooleanType, UnicodeType, ComplexType
SetType = type(set())

from struct import pack, unpack, unpack_from
from itertools import groupby

stypes = {
    LongType:"q",
    IntType:"i",
    FloatType:"d",
    StringType:'',
    NoneType:"f",
    BooleanType:"?",
    UnicodeType:'',
}

NAN = pack('f',float('nan'))

def dumps(l):
    if type(l) != list:
        raise ValueError, "Type not supported"

    st = ['=']
    stappend = st.append
    l1 = []
    l1extend = l1.extend
    l1append = l1.append
    for t,v in groupby(l, type):
        if t is str:
            for x in v:
                l1append(x)
                stappend(str(len(x))+'s')
        elif t is unicode:
            for x in v:
                v1=x.encode('utf8')
                l1append(v1)
                stappend(str(len(v1))+'s')
        elif t is NoneType:
            lenv = 0
            for _ in v:
                l1append(NAN)
                lenv += 1
            stappend( str(lenv)+'f' if lenv>1 else 'f')
        else:
            lenv = 0
            f = stypes[t]
            for x in v:
                l1append(pack(f,x))
                lenv += 1
            stappend( str(lenv)+f if lenv>1 else f )

    st = ''.join(st)
    return pack('!I', len(st)) + st + ''.join(l1)

def loads(s):
    stp = unpack('!I', s[:4])[0] + 4
    return [None if x!=x else x for x in unpack_from(s[4:stp], s, stp)]

if __name__ == "__main__":
    #l1 = [1,3,4,"lala", "gaga", 5, "as" , None,None,u'asfdasdf', u'qwerqewr', 3,4,5,None, None, "LALAKIS"]
    l1 = ['lal',3]
    print l1
    a=dumps(l1)
    print loads(a)