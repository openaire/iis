from oozie2dot.core import *

__author__ = "Mateusz Kobos mkobos@icm.edu.pl"

def handle_fork(elem, names_register):
    """
    @param elem: element to be processed
    @type elem: xml.etree.ElementTree.Element
    """
    label = elem.get('name')
    start = names_register.get(label)
    print_node(start, labels=[label], color='green')
    ends = []
    for found in elem.findall('path'):
        ends.append(names_register.get(found.get('start')))
    print_edges(start, ends)

def handle_join(elem, names_register):
    label = elem.get('name')
    start = names_register.get(label)
    print_node(start, labels=[label], color='green')
    print_edges(start, [names_register.get(elem.get('to'))])

def handle_decision(elem, names_register):
    label = elem.get('name')
    start = names_register.get(label)
    print_node(start, labels=[label], color='yellow')
    for switch in elem.findall('switch'):
        for case in switch:
            tag = case.tag
            end = names_register.get(case.get('to'))
            if tag == 'case':
                text = case.text
                print_edges(start, [end], label=text)
            elif tag == 'default':
                print_edges(start, [end], label='default')
            else:
                raise Exception('Unknown case tag "{}"'.format(tag))

def handle_start(elem, names_register):
    print_edges('start', [names_register.get(elem.get('to'))])

def handle_end(elem, names_register):
    label = elem.get('name')
    start = names_register.get(label)
    print_node(start, labels=[label])
