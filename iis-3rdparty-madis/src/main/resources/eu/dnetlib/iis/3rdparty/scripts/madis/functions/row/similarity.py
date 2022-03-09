from . import setpath
import lib.jopts as jopts
import functions
import math


#def cosine(*args):
#    """
#
#    .. function:: cosine(pack1,pack2)
#
#    Returns cosine distance value of two packs ( vector-like object's created by :func:`~functions.aggregate.packing.pack` and :func:`~functions.aggregate.packing.vecpack` functions).
#
#    Example:
#
#    Calculate distance in user movies WITH preference score.
#
#    >>> table2('''
#    ... user1   Jonny 10
#    ... user1   Movie 5
#    ... user1   Depp 8
#    ... user1   Russel 3
#    ... user2   Jonny 1
#    ... user2   Movie 1
#    ... user2   Depp 2
#    ... ''')
#
#
#    Creating every user's pack over movies (column b) with dimension values (preference) column c.
#
#    >>> sql(\"""create table tmpuserfullvecsInmovie as
#    ...         select a as userid, pack(b,c) as pk
#    ...             from table2 group by a\""")
#
#    Use packs to calculate cosine distance over user packs.
#
#    >>> sql(\"""select u1.userid,u2.userid, cosine(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuserfullvecsInmovie as u1,
#    ...             tmpuserfullvecsInmovie as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    --------------------------------
#    user1  | user2  | 0.899401222438
#
#.. doctest::
#    :hide:
#
#    >>> sql("drop table tmpuserfullvecsInmovie")
#
#    """
#    if len(args)!=2:
#        raise functions.OperatorError("cosine","operator takes exactly two arguments")
#    try:
#        r=memunpack(args[0])
#        s=memunpack(args[1])
#    except Exception,e:
#        raise functions.OperatorError("cosine","Wrong format in operator arguments: %s" %(e))
#    rset=set(r) #set(r)
#    sset=set(s) #set(s)#
#    sumr=math.sqrt(sum([float(r[key])**2 for key in  rset ]))
#    sums=math.sqrt(sum([float(s[key])**2 for key in  sset ]))
#    return sum([float(r[key]*s[key])  for key in  (rset & sset )])/(sumr*sums)
#
#cosine.registered=True


def jaccard(*args):
    """
    .. function:: jaccard(jpack1,jpack2)

    Return jaccard similarity value of two jpacks.

    Example:

    >>> table1('''
    ... user1   movie1 20
    ... user1   movie2 30
    ... user2   movie1 40
    ... user2   movie3 90
    ... user2   movie4 90
    ... user3   movie1 40
    ... user3   movie3 80
    ... user4   movie1 70
    ... user4   movie2 10
    ... ''')

    NOTE that only column b is jgrouped because *jaccard* operates on packs as sets, not weighted values, So packing
    also column c would not make any difference.

    >>> sql(\"""select u1.userid,u2.userid, jaccard(u1.pk, u2.pk) as similarity
    ...     from
    ...         (select a as userid, jgroup(b)  as pk from table1 group by a) as u1,
    ...         (select a as userid, jgroup(b) as pk from table1 group by a) as u2
    ...     where u1.userid<u2.userid\""")
    userid | userid | similarity
    --------------------------------
    user1  | user2  | 0.25
    user1  | user3  | 0.333333333333
    user1  | user4  | 1.0
    user2  | user3  | 0.666666666667
    user2  | user4  | 0.25
    user3  | user4  | 0.333333333333
    """

    if len(args)!=2:
        raise functions.OperatorError("jaccard","operator takes exactly two arguments")
    try:
        r=jopts.fromj(args[0])
        s=jopts.fromj(args[1])
    except Exception as e:
        raise functions.OperatorError("jaccard"," Wrong format arguments: %s" %(e))
    rset=set([tuple(x) if type(x)==list else x for x in r])
    sset=set([tuple(x) if type(x)==list else x for x in s])

    return float(len( rset & sset ))/(len( rset | sset ))

jaccard.registered=True


def sorensendice(*args):
    """
    .. function:: sorensendice(jpack1,jpack2)

    Return jaccard similarity value of two jpacks.

    Example:

    >>> table1('''
    ... user1   movie1 20
    ... user1   movie2 30
    ... user2   movie1 40
    ... user2   movie3 90
    ... user2   movie4 90
    ... user3   movie1 40
    ... user3   movie3 80
    ... user4   movie1 70
    ... user4   movie2 10
    ... ''')

    NOTE that only column b is jgrouped because *jaccard* operates on packs as sets, not weighted values, So packing
    also column c would not make any difference.

    >>> sql(\"""select u1.userid,u2.userid, sorensendice(u1.pk, u2.pk) as similarity
    ...     from
    ...         (select a as userid, jgroup(b)  as pk from table1 group by a) as u1,
    ...         (select a as userid, jgroup(b) as pk from table1 group by a) as u2
    ...     where u1.userid<u2.userid\""")
    userid | userid | similarity
    ----------------------------
    user1  | user2  | 0.4
    user1  | user3  | 0.5
    user1  | user4  | 1.0
    user2  | user3  | 0.8
    user2  | user4  | 0.4
    user3  | user4  | 0.5
    """

    if len(args)!=2:
        raise functions.OperatorError("sorensendice","operator takes exactly two arguments")
    try:
        r=jopts.fromj(args[0])
        s=jopts.fromj(args[1])
    except Exception as e:
        raise functions.OperatorError("sorensendice"," Wrong format arguments: %s" %(e))
    rset=set([tuple(x) if type(x)==list else x for x in r])
    sset=set([tuple(x) if type(x)==list else x for x in s])

    return 2 * float(len( rset & sset ))/(len(rset) + len(sset) )

sorensendice.registered=True

#def euclean(*args):###not working with lists
#    """
#
#    .. function:: euclean(pack1,pack2)
#
#    Returns euclidean distance value of two packets ( vector-like object's created by :func:`~functions.aggregate.packing.pack` and :func:`~functions.aggregate.packing.vecpack` function).
#    If packets are full vectors then then non-common metrics WILL be used in the calculation. To avoid that do not include zero values
#    in *pack1* and *pack2* or use :func:`eucleancommon`, but this solution is slower.
#
#    Example:
#
#    >>> table1('''
#    ... user1   movie1 20
#    ... user1   movie2 30
#    ... user2   movie1 40
#    ... user2   movie3 90
#    ... user2   movie4 90
#    ... user3   movie1 40
#    ... user3   movie3 80
#    ... user4   movie1 70
#    ... user4   movie2 10
#    ... ''')
#
#    Creating in table tmpuservecsInmovie user packs over movies (column b) as sets (no preference score is packed - column c).
#
#    >>> sql(\"""create table tmpuservecsInmovie as
#    ...         select a as userid, pack(b) as pk
#    ...             from table1 group by a\""")
#
#    Creating vectorizing packs in table tmpuserfullvecsInmovie again as sets.
#    Every user vector pack includes all distinct values of column b as dimension
#    with weight 1 or 0 based on if the dimension exists in user group in the table2.
#
#    >>> sql(\"""create table tmpuserfullvecsInmovie as
#    ...         select a as userid, vecpack(metrics,b) as pk
#    ...             from
#    ...                 table1,
#    ...                 (select pack(distinct b) as metrics from table1)
#    ...             group by a\""")
#
#    Calculating euclean over non-vector packs that do not include preference score (column c) leads to zero distance for all user pairs,
#    since in common dimension value 1 is included in the pack.
#
#    >>> sql(\"""select u1.userid,u2.userid, euclean(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuservecsInmovie as u1,
#    ...             tmpuservecsInmovie as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    --------------------------
#    user1  | user2  | 0.0
#    user1  | user3  | 0.0
#    user1  | user4  | 0.0
#    user2  | user3  | 0.0
#    user2  | user4  | 0.0
#    user3  | user4  | 0.0
#
#    Calculating euclean over vector packs that do not include preference score (column c) leads to non-zero distance for some user pairs,
#    since all dimensions are used in the calculation.
#
#    >>> sql(\"""select u1.userid,u2.userid, euclean(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuserfullvecsInmovie as u1,
#    ...             tmpuserfullvecsInmovie as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    -------------------------------
#    user1  | user2  | 1.73205080757
#    user1  | user3  | 1.41421356237
#    user1  | user4  | 0.0
#    user2  | user3  | 1.0
#    user2  | user4  | 1.73205080757
#    user3  | user4  | 1.41421356237
#
#    The example above demonstrates the different behavior of euclean function based on the input packs (vectors or not), but
#    it isn't very useful because packs include only dimensions and not weights that is the case that euclean is commonly used.
#
#    The exampe below follows the same steps as the above but includes score (column c) in the packs.
#
#    >>> sql(\"""create table tmpuservecsInmoviePref as
#    ...         select a as userid, pack(b,c) as pk
#    ...             from table1 group by a\""")
#    >>> sql(\"""create table tmpuserfullvecsInmoviePref as
#    ...         select a as userid, vecpack(metrics,b,c) as pk
#    ...             from
#    ...                 table1,
#    ...                 (select pack(distinct b) as metrics from table1)
#    ...             group by a\""")
#    >>> sql(\"""select u1.userid,u2.userid, euclean(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuservecsInmoviePref as u1,
#    ...             tmpuservecsInmoviePref as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    -------------------------------
#    user1  | user2  | 20.0
#    user1  | user3  | 20.0
#    user1  | user4  | 53.8516480713
#    user2  | user3  | 10.0
#    user2  | user4  | 30.0
#    user3  | user4  | 30.0
#    >>> sql(\"""select u1.userid,u2.userid, euclean(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuserfullvecsInmoviePref as u1,
#    ...             tmpuserfullvecsInmoviePref as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    -------------------------------
#    user1  | user2  | 132.287565553
#    user1  | user3  | 87.7496438739
#    user1  | user4  | 53.8516480713
#    user2  | user3  | 90.5538513814
#    user2  | user4  | 131.148770486
#    user3  | user4  | 86.0232526704
#
#.. doctest::
#    :hide:
#
#    >>> sql("drop table tmpuservecsInmovie")
#    >>> sql("drop table tmpuserfullvecsInmovie")
#    >>> sql("drop table tmpuservecsInmoviePref")
#    >>> sql("drop table tmpuserfullvecsInmoviePref")
#    """
#    if len(args)!=2:
#        raise functions.OperatorError("euclean","operator takes exactly two arguments")
#    try:
#        r=memunpack(args[0])
#        s=memunpack(args[1])
#    except Exception,e:
#        raise functions.OperatorError("euclean","Wrong format operator arguments: %s" %(e))
#    rset=set(r)
#    sset=set(s)
#    return math.sqrt(sum([(float(r[key])-float(s[key]))**2 for key in  (rset & sset )]))
#euclean.registered=True


#def eucleancommon(*args):###not working with lists
#    """
#
#    .. function:: eucleancommon(pack1,pack2)
#
#    Returns euclidean distance value of two packets ( vector-like object's created by :func:`~functions.aggregate.packing.pack` and :func:`~functions.aggregate.packing.vecpack` function).
#    If packets are full vectors or include zero weights then then only common metrics (non-zero values) will be used in the calculation.
#    This function behaves exactly like :func:`euclean` but exhibits the same behavior if packs are vectorized.
#
#    Examples:
#
#    The examples used are the same ones used in :func:`euclean`. Check them for a more detailed explanation of the followed steps.
#
#    >>> table1('''
#    ... user1   movie1 20
#    ... user1   movie2 30
#    ... user2   movie1 40
#    ... user2   movie3 90
#    ... user2   movie4 90
#    ... user3   movie1 40
#    ... user3   movie3 80
#    ... user4   movie1 70
#    ... user4   movie2 10
#    ... ''')
#
#    Calculate distance in user movies WITHOUT preference score.
#
#    >>> sql(\"""create table tmpuservecsInmovie as
#    ...         select a as userid, pack(b) as pk
#    ...             from table1 group by a\""")
#    >>> sql(\"""create table tmpuserfullvecsInmovie as
#    ...         select a as userid, vecpack(metrics,b) as pk
#    ...             from
#    ...                 table1,
#    ...                 (select pack(distinct b) as metrics from table1)
#    ...             group by a\""")
#    >>> sql(\"""select u1.userid,u2.userid, eucleancommon(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuservecsInmovie as u1,
#    ...             tmpuservecsInmovie as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    --------------------------
#    user1  | user2  | 0.0
#    user1  | user3  | 0.0
#    user1  | user4  | 0.0
#    user2  | user3  | 0.0
#    user2  | user4  | 0.0
#    user3  | user4  | 0.0
#    >>> sql(\"""select u1.userid,u2.userid, eucleancommon(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuserfullvecsInmovie as u1,
#    ...             tmpuserfullvecsInmovie as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    --------------------------
#    user1  | user2  | 0.0
#    user1  | user3  | 0.0
#    user1  | user4  | 0.0
#    user2  | user3  | 0.0
#    user2  | user4  | 0.0
#    user3  | user4  | 0.0
#
#    Calculate distance in user movies WITH preference score.
#
#    >>> sql(\"""create table tmpuservecsInmoviePref as
#    ...         select a as userid, pack(b,c) as pk
#    ...             from table1 group by a\""")
#    >>> sql(\"""create table tmpuserfullvecsInmoviePref as
#    ...         select a as userid, vecpack(metrics,b,c) as pk
#    ...             from
#    ...                 table1,
#    ...                 (select pack(distinct b) as metrics from table1)
#    ...             group by a\""")
#    >>> sql(\"""select u1.userid,u2.userid, eucleancommon(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuservecsInmoviePref as u1,
#    ...             tmpuservecsInmoviePref as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    -------------------------------
#    user1  | user2  | 20.0
#    user1  | user3  | 20.0
#    user1  | user4  | 53.8516480713
#    user2  | user3  | 10.0
#    user2  | user4  | 30.0
#    user3  | user4  | 30.0
#    >>> sql(\"""select u1.userid,u2.userid, eucleancommon(u1.pk,u2.pk) as distance
#    ...         from
#    ...             tmpuserfullvecsInmoviePref as u1,
#    ...             tmpuserfullvecsInmoviePref as u2
#    ...         where u1.userid<u2.userid\""")
#    userid | userid | distance
#    -------------------------------
#    user1  | user2  | 20.0
#    user1  | user3  | 20.0
#    user1  | user4  | 53.8516480713
#    user2  | user3  | 10.0
#    user2  | user4  | 30.0
#    user3  | user4  | 30.0
#
#.. doctest::
#    :hide:
#
#    >>> sql("drop table tmpuservecsInmovie")
#    >>> sql("drop table tmpuserfullvecsInmovie")
#    >>> sql("drop table tmpuservecsInmoviePref")
#    >>> sql("drop table tmpuserfullvecsInmoviePref")
#    """
#    if len(args)!=2:
#        raise functions.OperatorError("eucleancommon","operator takes exactly two arguments")
#    try:
#        r=memunpack(args[0])
#        s=memunpack(args[1])
#    except Exception,e:
#        raise functions.OperatorError("eucleancommon","Wrong format in operator arguments: %s" %(e))
#    rset=set([key for key in r if r[key]!=0]) #set(r)
#    sset=set([key for key in s if s[key]!=0]) #set(s)#
#
#    return math.sqrt(sum([(float(r[key])-float(s[key]))**2 for key in  (rset & sset )]))
#
#eucleancommon.registered=True




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
