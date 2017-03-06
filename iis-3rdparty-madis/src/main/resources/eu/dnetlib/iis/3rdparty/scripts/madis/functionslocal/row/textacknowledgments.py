# coding: utf-8
import setpath
import re
from lib import porter2 as porter
import functions
import unicodedata
import itertools

# Increase regular expression cache
try:
    re._MAXCACHE = 1000
except:
    pass

# Every regular expression containing \W \w \D \d \b \S \s needs to be compiled
# like below. If you want to embed the UNICODE directive inside the
# regular expression use:
# (?u) like re.sub(ur'(?u)[\W\d]', ' ', o)
delete_numbers_and_non_letters=re.compile(ur'[\W]',re.UNICODE)
delete_non_letters=re.compile(ur'[\W]',re.UNICODE)
delete_word_all=re.compile(ur'\w+\sall',re.UNICODE)
delete_word_all_and_or=re.compile(ur'\w+\sall\s(?:and|or)',re.UNICODE)
text_tokens = re.compile(ur'([\d.]+\b|\w+|\$[\d.]+)', re.UNICODE)
strip_remove_newlines=re.compile(u'(?:\\s+$|^\\s+|(?<=[^\\s\\d\\w.;,!?])\n+)', re.UNICODE)
reduce_spaces=re.compile(ur'\s+', re.UNICODE)
cqlterms=('title', 'subject', 'person', 'enter', 'creator', 'isbn')
replchars = re.compile(r'[\n\r]')


def textacknowledgments(txt,span = 10,maxlen = 3,pattern = r'(?:support)|(?:thank)|(?:in part)|(?:research)|(?:\bwork\b)|(?:\bgrants?\b)|(?:project)|(?:science)|(?:fund)|(?:nation)|(?:author)|(?:foundation)|(?:\bprogram\b)|(?:\bhelp\b)|(?:paper)|(?:technolog)|(?:partial)|(?:acknowledg)|(?:provid)|(?:grate)|(?:\bcenter\b)|(?:study)|(?:discuss)|(?:particip)|(?:ministry)|(?:contribut)|(?:european)|(?:number)|(?:valuabl)|(?:education)|(?:council)|(?:award)|(?:contract)|(?:institut)' ):
    """
    .. function:: textacknowledgments(text, span = 10, maxlen = 5, pattern = (\b|_)(1|2)\d{3,3}(\b|_))

    Returns the "Reference" section of documents. To find it, it searches for parts of the document that
    have a high density of pattern matches.

    .. parameters:: txt,maxlen,pattern
       txt: input text.
       span: the size of the string in words that the txt is splited
       maxlen: the size of the scrolling window over the text in which the density is calculated.
       pattern: regular expression that is matched against the lines of the text. By default the pattern matches
                year occurences so as to extract sections that look like references.

    
    Examples:

    >>> sql("select textacknowledgments('')")
    textacknowledgments('')
    ------------------
    <BLANKLINE>
    """

    exp = re.sub('\r\n','\n',txt)
    exp = reduce_spaces.sub(' ', strip_remove_newlines.sub('', exp))

    if exp.count(' ') < span * 10:
        return exp

    acknowledgments = []
    origwords = exp.split(' ')
    words = exp.lower()
    words = words.split(' ')
    stemed = []
    # for k in words:
    #     if len(k) > 0:
    #         stemed.append(porter.stem(k))
    spanedorigtext = [' '.join(origwords[i:i+span]) for i in range(0, len(origwords), span)]
    spanedstemtext = [' '.join(words[i:i+span]) for i in range(0, len(words), span)]
    reversedtext = iter(spanedstemtext)
    results = []
    densities = []

    for i in xrange(maxlen/2):
        results.append(0)
    for i in reversedtext:
        count = sum(1 for m in re.finditer(pattern, i))
        if count:
                results.append(count)
        else:
                results.append(0)

    for i in xrange(maxlen/2):
        results.append(0)

    #print len(spanedorigtext), len(spanedstemtext), len(results), len(results)-maxlen/2 - maxlen/2

    out = 0
    temp = 0
    for i in xrange(maxlen/2,len(results)-maxlen/2):
        densities.append(sum(results[i-maxlen/2:i-maxlen/2+maxlen])*1.0/maxlen)

    # for cnt, i in enumerate(spanedorigtext):
    #     print i, results[maxlen/2+cnt], densities[cnt]

    threshold = 1

    paragraphsum = []
    paragraphs = []
    prev = -10
    current = 0
    maxsum = 0
    maxi = 0
    for line in spanedorigtext:
        if densities[current] > threshold:
            # new paragraph first visit
            if (prev+1) != current:
                paragraphsum.append(0)
                paragraphs.append([])
            paragraphsum[-1] += results[maxlen/2+current]
            paragraphs[-1].append(line)
            prev = current
        current += 1

    for cnt, paragraph in enumerate(paragraphs):
        if paragraphsum[cnt] > maxsum:
            maxsum = paragraphsum[cnt]
            maxi = cnt
    #     print '\n'.join(paragraph), paragraphsum[cnt], '\n'
    # print '!!!!!!!!', maxsum, maxi

    paragraphsum.append(0)
    paragraphs.append([])
    if paragraphsum[maxi] > 2:
        return '\n'.join(paragraphs[maxi])
        #return ('\n'.join(paragraphs[maxi]))+" "+str(paragraphsum[maxi])
    else:
        return ''

textacknowledgments.registered=True


def textacknowledgmentsstem(txt,span = 10,maxlen = 3,pattern = r'(?:support)|(?:thank)|(?:research)|(?:\bwork\b)|(?:\bgrant\b)|(?:project)|(?:scienc)|(?:\bfund\b)|(?:nation)|(?:author)|(?:foundat)|(?:\bprogram\b)|(?:\bhelp\b)|(?:univers)|(?:paper)|(?:technolog)|(?:partial)|(?:comment)|(?:develop)|(?:acknowledg)|(?:review)|(?:provid)|(?:grate)|(?:\bcenter\b)|(?:studi)|(?:discuss)|(?:particip)|(?:ministri)|(?:contribut)|(?:european)|(?:system)|(?:comput)|(?:number)|(?:valuabl)|(?:educ)|(?:council)|(?:award)|(?:contract)|(?:inform)|(?:institut)' ):
    """
    .. function:: textacknowledgmentsstem(text, span = 10, maxlen = 5, pattern = (\b|_)(1|2)\d{3,3}(\b|_))

    Returns the "Reference" section of documents. To find it, it searches for parts of the document that
    have a high density of pattern matches.

    .. parameters:: txt,maxlen,pattern
       txt: input text.
       span: the size of the string in words that the txt is splited
       maxlen: the size of the scrolling window over the text in which the density is calculated.
       pattern: regular expression that is matched against the lines of the text. By default the pattern matches
                year occurences so as to extract sections that look like references.


    Examples:

    >>> sql("select textacknowledgmentsstem('')")
    textacknowledgmentsstem('')
    ------------------
    <BLANKLINE>
    """

    exp = re.sub('\r\n','\n',txt)
    exp = reduce_spaces.sub(' ', strip_remove_newlines.sub('', exp))

    if exp.count(' ') < span * 10:
        return exp

    acknowledgments = []
    origwords = exp.split(' ')
    words = exp.lower()
    words = words.split(' ')
    stemed = []
    for k in words:
        if len(k) > 0:
            try:
                stemed.append(porter.stem(k))
            except Exception:
                stemed.append(k)
    spanedorigtext = [' '.join(origwords[i:i+span]) for i in range(0, len(origwords), span)]
    spanedstemtext = [' '.join(stemed[i:i+span]) for i in range(0, len(stemed), span)]
    reversedtext = iter(spanedstemtext)
    results = []
    densities = []

    for i in xrange(maxlen/2):
        results.append(0)
    for i in reversedtext:
        count = sum(1 for m in re.finditer(pattern, i))
        if count:
                results.append(count)
        else:
                results.append(0)

    for i in xrange(maxlen/2):
        results.append(0)

    for i in xrange(maxlen/2,len(results)-maxlen/2):
        densities.append(sum(results[i-maxlen/2:i-maxlen/2+maxlen])*1.0/maxlen)

    threshold = 1

    current = 0
    for i in spanedorigtext:
        if len(i)>10:
            if densities[current] > threshold:
                acknowledgments.append(i)
            current+=1
    return '\n'.join(acknowledgments)

textacknowledgmentsstem.registered=True


def textacknowledgmentstara(txt, span=13, maxlen=3,
                        pattern=r'(?:\bthank)|(?:\btara\b)|(?:\barticl)|(?:\bocean\b)|(?:\bpmc\b)|(?:\bsupport)|(?:\bsampl)|(?:\bexpedit)|(?:\bfoundat)|(?:\bresearch)|(?:\bhelp)|(?:\binstitut)|(?:\bmarin)|(?:\bnation)|(?:\backnowledg)|(?:\bcomment)|(?:\bcontribut)|(?:\bfund)|(?:\bgrate)|(?:\bprovid)|(?:\bproject)|(?:\bpossibl)|(?:\bscienc)|(?:author)|(?:grant)|(?:fellowship)|(?:program)|(?:programm)|(?:suggest)|(?:taraexpedit)|(?:université)|(?:valuabl)|(?:without)|(?:pmc articles)|(?:tara oceans)|(?:oceans expedition)|(?:oceans consortium)|(?:anonymous reviewers)|(?:article contribution)|(?:environment foundation)|(?:people sponsors)|(?:projects? poseidon)|(?:wish thank)|(?:commitment following)|(?:continuous support)|(?:data analysis)|(?:exist without)|(?:tara girus)|(?:tara schooner)|(?:keen thank)|(?:oceans taraexpeditions)|(?:possible thanks)|(?:sponsors made)|(?:technical assistance)|(?:thank commitment)|(?:without continuous)'):
    """
    .. function:: textacknowledgments(text, span = 10, maxlen = 5, pattern = (\b|_)(1|2)\d{3,3}(\b|_))

    Returns the "Reference" section of documents. To find it, it searches for parts of the document that
    have a high density of pattern matches.

    .. parameters:: txt,maxlen,pattern
       txt: input text.
       span: the size of the string in words that the txt is splited
       maxlen: the size of the scrolling window over the text in which the density is calculated.
       pattern: regular expression that is matched against the lines of the text. By default the pattern matches
                year occurences so as to extract sections that look like references.


    Examples:

    >>> sql("select textacknowledgments('')")
    textacknowledgments('')
    ------------------
    <BLANKLINE>
    """

    exp = re.sub('\r\n', '\n', txt)
    exp = reduce_spaces.sub(' ', strip_remove_newlines.sub('', exp))

    if exp.count(' ') < span * 10:
        return exp

    acknowledgments = []
    origwords = exp.split(' ')
    words = exp.lower()
    words = words.split(' ')
    stemed = []
    # for k in words:
    #     if len(k) > 0:
    #         stemed.append(porter.stem(k))
    spanedorigtext = [' '.join(origwords[i:i + span]) for i in range(0, len(origwords), span)]
    spanedstemtext = [' '.join(words[i:i + span]) for i in range(0, len(words), span)]
    reversedtext = iter(spanedstemtext)
    results = []
    densities = []

    for i in xrange(maxlen / 2):
        results.append(0)
    for i in reversedtext:
        count = sum(1 for m in re.finditer(pattern, i))
        if count:
            results.append(count)
        else:
            results.append(0)

    for i in xrange(maxlen / 2):
        results.append(0)

    # print len(spanedorigtext), len(spanedstemtext), len(results), len(results)-maxlen/2 - maxlen/2

    out = 0
    temp = 0
    for i in xrange(maxlen / 2, len(results) - maxlen / 2):
        densities.append(sum(results[i - maxlen / 2:i - maxlen / 2 + maxlen]) * 1.0 / maxlen)

    # for cnt, i in enumerate(spanedorigtext):
    #     print i, results[maxlen/2+cnt], densities[cnt]

    threshold = 1

    paragraphsum = []
    paragraphs = []
    prev = -10
    current = 0
    maxsum = 0
    maxi = 0
    for line in spanedorigtext:
        if densities[current] > threshold:
            # new paragraph first visit
            if (prev + 1) != current:
                paragraphsum.append(0)
                paragraphs.append([])
            paragraphsum[-1] += results[maxlen / 2 + current]
            paragraphs[-1].append(line)
            prev = current
        current += 1

    for cnt, paragraph in enumerate(paragraphs):
        if paragraphsum[cnt] > maxsum:
            maxsum = paragraphsum[cnt]
            maxi = cnt
    # print '\n'.join(paragraph), paragraphsum[cnt], '\n'
    # print '!!!!!!!!', maxsum, maxi

    paragraphsum.append(0)
    paragraphs.append([])
    if paragraphsum[maxi] > 2:
        return '\n'.join(paragraphs[maxi])
        # return ('\n'.join(paragraphs[maxi]))+" "+str(paragraphsum[maxi])
    else:
        return ''


textacknowledgmentstara.registered = True


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
