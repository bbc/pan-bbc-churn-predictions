from tests.fixtures import *


class StoreInModule:
    """
    Enables inheritance through tests
    probably not best practice but helps when
    methods of a class are designed to be chained
    prevents reinstantiation of class each time.
    """
    initial = "value"
    new_value = None