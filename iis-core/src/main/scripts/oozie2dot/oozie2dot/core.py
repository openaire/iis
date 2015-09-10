from __future__ import print_function

__author__ = "Mateusz Kobos mkobos@icm.edu.pl"

import sys
import re

class LabelsRegister:
    """
    Register that assigns and stores mapping between user-readable labels and 
    technical identifiers used in the *.dot file.
    """
    def __init__(self):
        self.__d = {}
        self.__largest = 0

    def get(self, label):
        """@return node identifier assigned to given label"""
        if label not in self.__d:
            self.__largest = self.__largest+1
            self.__d[label] = self.__largest
        index = self.__d[label]
        return 'n'+str(index)

def normalize_label(label):
    text = label
    text = text.replace('"', '\'')
    pattern = re.compile(r'\s+')
    text = re.sub(pattern, ' ', text)
    return text

def print_error(text):
    print(text, file=sys.stderr)

def print_edges(start, ends, label=None):
    text = start+' -> {'+' '.join(ends)+'}'
    parameters = []
    if label is not None:
        parameters.append('label="{}"'.format(normalize_label(label)))
    if len(parameters) == 0:
        print(text)
    else:
        print('{}[{}]'.format(text, ' '.join(parameters)))

def print_node(name, labels=None, color=None, shape=None):
    """
    @param labels: list of labels to be printed in the node. Each label is placed
        in a separate line
    @param color: string taken from http://graphviz.org/doc/info/colors.html
    @param shape: string taken from http://graphviz.org/doc/info/shapes.html
    """
    if (labels is None) and (color is None) and (shape is None):
        return
    params = []
    if labels is not None:
        text = []
        for label in labels:
            text.append(normalize_label(label))
        params.append('label="{}"'.format(r'\n'.join(text)))
    if color is not None:
        params.append('fillcolor={},style=filled'.format(color))
    if shape is not None:
        params.append('shape={}'.format(shape))
    print('{}[{}]'.format(name, ' '.join(params))) 
