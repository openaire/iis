def unistr(s):
    import types
    if type(s)==bytes:
        return str(s,'utf-8')
    if type(s)==str:
        return s
    else:
        return str(s)