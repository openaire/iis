def latinnum(x):
    x=int(x)
    if x<=0:
       raise ValueError("Value must be greater than zero to produce an equivalent latin string")
    lx=""
    while x>25:
        lx+=chr(ord('A')+int(x/25))
        x%=25
    lx+=chr(ord('A')+x)
    return lx