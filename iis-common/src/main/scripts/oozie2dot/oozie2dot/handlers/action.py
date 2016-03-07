from oozie2dot.core import *

__author__ = "Mateusz Kobos mkobos@icm.edu.pl"

def _get_action_type(elem):
    for child in elem:
        if child.tag not in ['ok', 'error']:
           return child.tag
    raise Exception('Action definition not found in elem={}'.format(elem))

class _ActionRepresentation:
    def __init__(self, labels, color, shape):
        self.labels = labels
        self.color = color
        self.shape = shape

def _get_action_representation(elem):
    type_ = _get_action_type(elem)
    label = elem.get('name')
    if type_ == 'sub-workflow':
        sub_workflow_elem = elem.find('sub-workflow')
        app_path = sub_workflow_elem.find('app-path').text
        labels = [label, 'app_path={}'.format(app_path)]
        return _ActionRepresentation(labels, 'cyan3', 'box3d')
    else:
        labels = [label, 'type={}'.format(type_)]
        return _ActionRepresentation(labels, 'cyan', 'box')

def handle_action(elem, names_register):
    label = elem.get('name')
    action_repr = _get_action_representation(elem)
    start = names_register.get(label)
    print_node(start, labels=action_repr.labels, color=action_repr.color, 
        shape=action_repr.shape)
    ok_elem = elem.find('ok')
    print_edges(start, [names_register.get(ok_elem.get('to'))])

