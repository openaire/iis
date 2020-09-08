# coding: utf-8

import setpath
import functions
import logging

def toggle(*args):

    """
    .. function:: toggle(setting_name)

    Toggles a boolean global setting

    Examples:

    >>> sql("toggle lala")
    toggle('lala')
    --------------
    lala not found

    >>> sql("toggle tracing")
    toggle('tracing')
    --------------------
    tracing is now: True
    """

    if len(args)==0:
        return
    setting=args[0].lower()
    if setting in functions.settings and type(functions.settings[setting])==bool:
        functions.settings[setting]^=True
        return setting+" is now: " + str(functions.settings[setting])
    else:
        return setting+" not found"
toggle.registered=True

def setting(*args):

    """
    .. function:: setting(setting_name, [value])

    Sets and returns a setting's value

    Examples:

    >>> sql("setting 'tracing' 0")
    setting('tracing','0')
    ----------------------
    False

    >>> sql("setting 'tracing'")
    setting('tracing')
    ------------------
    False
    """

    if len(args)==0:
        return str(functions.settings)
    setting=args[0].lower()
    if setting in functions.settings:
        if len(args)==1:
            return str(functions.settings[setting])
        elif len(args)==2:
            if type(functions.settings[setting])==bool:
                if args[1].lower() in ['true', '1']:
                    s=True
                else:
                    s=False
                functions.settings[setting]=s
            elif type(functions.settings[setting])==int:
                functions.settings[setting]=int(args[1])
            elif type(functions.settings[setting])==str:
                functions.settings[setting]=str(args[1])
            return str(functions.settings[setting])
    else:
        return setting+" not found"
setting.registered=True

def setlog(*args):

    """
    .. function:: setlog(filename)

    Sets the log file path/filename for exec operator

    """

    setting='logging'
    if functions.settings[setting]:
        return True

    functions.settings[setting]=True
    if len(args)==0:
        file=None
    else:
        file=args[0]

    logging.basicConfig(filename=file,level=logging.NOTSET,format="%(asctime)s - %(name)s - %(flowname)s - %(levelname)s - %(message)s")
    return True

setlog.registered=True


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
