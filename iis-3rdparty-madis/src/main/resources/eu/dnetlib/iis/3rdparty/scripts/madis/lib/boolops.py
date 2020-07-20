
def xor(*args):
    if sum([bool(a) for a in args])==1:
        return True
    return False