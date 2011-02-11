# Copyright 2006 Uberan - All Rights Reserved

## Notes on Memory usage (for 64-bit Linux):
# (Nothing)                             (14 bytes)   
# An int                                 24 bytes
# A float                                24 bytes
# A tuple with one int:                  87 bytes
# A subclassed tuple with one int:      103 bytes
# An old-style class one int member:    369 bytes
# A new-style class one int member:     361 bytes
# A dict with one entry (int -> int)    322 bytes              
# A list with one int                   125 bytes
# A Record with one int:                103 bytes
# A Record with one int and old classes 103 bytes
# A Record with one int and new classes 103 bytes

# An old-style class with no members:   346 bytes
# A new-style class with one method:     64 bytes
# A new-style class with no members:     64 bytes
# An empty dict:                         68 bytes
# An empty list:                         69 bytes
# An empty tuple without subclassing:    10 bytes
# An empty subclassed tuple:             69 bytes
# An empty Record:                       69 bytes
# An emtpy Record with old classes:      69 bytes
# An emtpy Record with new classes:      69 bytes

## Morals of the memory tests:
# 1. If you need memory efficient objects, subclassing tuple or using
# Record is good
# 2. Mixing in other classes for more methods is safe.  
# 3. If you really need it to be more compact, use pure tuples

def Record(*slots, **kargs):
    base = kargs.get("base", RecordBase)
    assert issubclass(base, RecordBase)
    return base.add_slots(*slots)

class RecordBase(tuple):
    SLOTS = ()

    ## Override this one instead of __init__.
    def __new__(cls, *values):
        assert len(values) == len(cls.SLOTS)
        return tuple.__new__(cls, values)

    ## Use this when overriding __new__
    @classmethod
    def new(cls, *values):
        assert len(values) == len(cls.SLOTS)
        return tuple.__new__(cls, values)

    def __repr__(self):
        return self.__class__.__name__ + repr(tuple(self.values))
    
    @property
    def values(self):
        return tuple.__iter__(self)

    @classmethod
    def from_values(cls, values):
      return cls.new(*values)


    @property
    def named_values(self):
        return ((self.SLOTS[index], value) 
                for (index, value) in enumerate(self.values))

    @classmethod
    def from_named_values(cls, named_values):
        return cls.from_dict(dict(named_values))

    @classmethod
    def from_dict(cls, dct, default = None):
        return cls.new(*(dct.get(slot, default) for slot in cls.SLOTS))


    def alter(self, **kargs):
        return self.new(*(kargs.get(self.SLOTS[index], value)
                          for (index, value) in enumerate(self.values)))
        
    ### For Building Subclasses ###
    @classmethod
    def add_slots(old_cls, *slots):
        class cls(old_cls):
            pass

        old_size = len(cls.SLOTS)
        cls.SLOTS += slots
        for index, slot in enumerate(slots):
            index += old_size

            getter = cls.make_getter(slot, index)
            setter = cls.make_setter(slot, index)

            setattr(cls, slot, property(fget = getter))
            setattr(cls, setter.__name__, setter)
            setattr(cls, getter.__name__, getter)

        cls.__name__ = "Record%s" % (cls.SLOTS,)
        return cls
        
    @classmethod
    def make_getter(cls, slot, index):
        def getter(self):
            return selfgettuple.__getitem__(self, index)

        getter.__name__ = "get_" + slot
        return getter
        
    @classmethod
    def make_setter(cls, slot, index):
        def setter(self, value):
            return self.new(*(value if cur_index == index else cur_value
                              for (cur_index, cur_value)
                              in enumerate(self.values)))

        setter.__name__ = "set_" + slot
        return setter

if __name__ == "__main__":
    pass
