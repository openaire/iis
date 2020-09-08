# coding: utf-8

import setpath
import socket,struct
import re



def ip2long(*args):

    """
    .. function:: ip2long(ip) -> int

    Converts a decimal dotted quad IP string to long integer IP format.
    It can take either one column of IP strings or 4 columns each having one
    part of the IP address.

    Examples:

    >>> sql("select ip2long('123.123.123.123')")
    ip2long('123.123.123.123')
    --------------------------
    2071690107
    >>> sql("select ip2long(123,123,123,123)")
    ip2long(123,123,123,123)
    ------------------------
    2071690107

    """

    if len(args)==1:
        try:
            return struct.unpack('!L',socket.inet_aton(args[0]))[0]
        except:
            return
    elif len(args)==4:
        return struct.unpack('!L',socket.inet_aton('.'.join([str(x) for x in args])))[0]
ip2long.registered=True

def long2ip(*args):
    """
    .. function:: long2ip(int) -> ip

    Convert longint IP to dotted quad string

    Examples:

    >>> sql("select long2ip('2071690107')")
    long2ip('2071690107')
    ---------------------
    123.123.123.123
    >>> sql("select long2ip(2071690107)")
    long2ip(2071690107)
    -------------------
    123.123.123.123

    """

    return socket.inet_ntoa(struct.pack('!L',int(args[0])))
long2ip.registered=True


def ip_prefix(*args):
    """
    .. function:: ip_prefix(ip, class_number) -> ip

    Returns the subnetwork class of an IP address.

    Examples:

    >>> sql("ip_prefix '123.34.24.54' ")
    ip_prefix('123.34.24.54')
    -------------------------
    4
    >>> sql("ip_prefix '123.34.24.54' '3'")
    ip_prefix('123.34.24.54','3')
    -----------------------------
    123.34.24
    >>> sql("ip_prefix '123.34.24.54' '2'")
    ip_prefix('123.34.24.54','2')
    -----------------------------
    123.34
    """

    if args[0]=='':
        return ''
    
    ipl=[int(x) for x in args[0].split('.')]

    if len(args)==1:
        return len(ipl)

    return '.'.join( [str(x) for x in ipl[0:int(args[1])] ] )

ip_prefix.registered=True

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
