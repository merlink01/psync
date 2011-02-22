# Copyright 2006 Uberan - All Rights Reserved

from Record import Record

class EnumValue(int):
    def __new__(cls, name, val):
        self = int.__new__(cls, val)
        self.name = name
        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        return "{0}={1}".format(self.name, int.__str__(self))


def Enum(*slots):
    class Enum(Record(*slots)):
        pass

    return Enum.from_values(EnumValue(name, index)
                            for index, name in enumerate(slots))

if __name__ == "__main__":
    colors = Enum("blue", "green", "red")
    print colors
    print repr(colors.blue)
    print str(colors.blue)
    print int(colors.blue)
    print colors.green == 1
