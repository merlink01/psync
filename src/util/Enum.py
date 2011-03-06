# Copyright (c) 2011, Peter Thatcher
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

# This file provides a simple Enum class which lets you do this:
#
# colors = Enum("red", "blue", "green")
#
# The values are ints, and colors.red == 0, but if you print them, you
# get the string "red=0".
#
# So an enum value is a named int, and an enum is a list of enum values.

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
