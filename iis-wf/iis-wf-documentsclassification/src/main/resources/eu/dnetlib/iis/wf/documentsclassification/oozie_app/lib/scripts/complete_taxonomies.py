#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
        classes = json.loads(line)
        for taxonomy in ['meshEuroPMCClasses', 'DDCClasses', 'WoSClasses', 'arXivClasses', 'ACMClasses']:
                if not taxonomy in classes['classes']:
                        classes['classes'][taxonomy] = None
        print(json.dumps(classes))
