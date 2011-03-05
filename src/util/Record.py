# Copyright (c) 2012, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This file makes it really easy to make "Records" like so:
#
# class FileInfo(Record("path", "size", "hash")):
#     pass
#
# It's very handy because records are:
# 1.  Easy to make.
# 2.  Immutable, with good setters.
# 3.  Easy to extend (you can even override __iter__).
# 4.  Easy to convert to and from either lists or maps.
# 5.  Correct for __eq__ and __ne__.
# 6.  Deconstructed just like tuples.
# 7.  Smaller (memory-wise) than normal objects.
# 8.  Nicer than named tuples (which don't have nice setters).

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
            return tuple.__getitem__(self, index)

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

    def __ne__(this, that):
        return not this == that
    
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
