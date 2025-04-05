from collections import OrderedDict as od


class NullInspector:
    def __init__(self):
        pass

    def __getattr__(self, name):
        return od()
