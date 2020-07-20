def unistr(s):
    import types
    if type(s)==types.StringType:
        return unicode(s,'utf-8')
    if type(s)==types.UnicodeType:
        return s
    else:
        return unicode(s)