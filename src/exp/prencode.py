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

# val      type size-size size bytes
# =================================
# int      i -[0-9]+
# float    f -[0-9]+([0-9]+)?
# bytes    r 
# unicode  u utf-8
# sequence s count vals
# map      m count keys-and-vals
# tagged   t typename val
# False    F
# True     T
# None     N

class SimpleType(Record("encode", "decode")):
    pass

def it(val):
    return val

Int = SimpleType(str, int)
Float = SimpleType(str, float)
RawBytes = SimpleType(it, it)

class EncodedBytes(Record("encoding")):
    def encode(self, decoded):
        return decoded.encode(self.encoding)

    def decode(self, encoded):
        return encoded.decode(self.encoding)

class Const(Record("val")):
    def encode(self, _):
        return ""

    def decode(self, _):
        return self.val

class Sequence:
    def encode(self, seq):
        # *** serialize
        encoded_vals = [serialize(val) for val in seq]
        encoded_vals.insert(0, serialize(len(encoded_vals)))
        # *** allow returning list/iterable
        return encoded_vals

    @into(list)
    def decode(self, bytes):
        # *** deserialize(bytes, type = None)
        count = deserialize(bytes, Int)
        for _ in xrange(count):
            # *** automatically move forward in stream or get bytes offset
            value = deserialize(bytes)
            yield value

class Map:
    def encode(self, map_val):
        @into(list)
        def encoded_vals():
            for key, val in map_val.iteritems():
                # *** serialize
                yield serialize(key)
                yield serialize(val)

        encoded_vals = encoded_vals()
        encoded_vals.insert(0, serialize(len(encoded_vals)))
        # *** allow returning list/iterable
        return encoded_vals

    # *** OrderedDict
    @into(OrderedDict)
    def decode(self, bytes):
        # *** deserialize(bytes, type = None)
        count = deserialize(bytes, Int)
        for _ in xrange(count):
            # *** automatically move forward in stream or get bytes offset
            key = deserialize(bytes, Unicode)
            value = deserialize(bytes)
            yield (key, value)


TYPES_BY_CODE = {"i" : Int,
                 "f" : Float,
                 "r" : RawBytes,
                 "u" : EncodedBytes("utf-8"),
                 "s" : Sequence(),
                 "m" : Map(),
                 "t" : Tagged(),
                 "N" : Const(None),
                 "T" : Const(True),
                 "F" : Const(False),
                 "x" : EncodedBytes("base64"),
                 "z" : CompressedBytes("gzip")}

def read_coded_bytes(stream):
    code = stream.read(1)
    size_size = int(stream.read(1))
    size = int(stream.read(size_size))
    bytes = stream.read(size)
    return code, bytes

class PrencodeSerializer:
    def serialize(self, val):
        state  = Referer()
        buffer = cStringIO.StringIO()
        self.encodeAny(val, buffer, state)
        return buffer.getvalue()

    def encodeAny(self, val, buffer, state):
        mark, encode = self.markAndEncoderByType.get(type(val), self.defaultMarkAndEncoder)
        return encode(self, mark, val, buffer, state)

    def encodeSimplifyable(self, mark, simplifyable, buffer, state):
        cls = simplifyable.__class__
        tag = self.namedClasses.nameFromClass(cls)
        if tag is None:
            raise Exception("Cannot encode values of type %s" % simplifyable.__class__.__name__)
        else:
            if cls in self.referenceClasses:
                reference, is_new = state.addReferent(simplifyable)
                if is_new:
                    self.encodeReferentHeader(reference, buffer, state)
                    values = simplifyable.toValues()
                    self.encodeCollection(mark, itertools.chain((tag,), values), buffer, state)
                else:
                    self.encodeReference(reference, buffer, state)
            else:
                values = simplifyable.toValues()
                self.encodeCollection(mark, itertools.chain((tag,), values), buffer, state)

    def encodeUnicode(self, mark, text, buffer, state):
        bytes = text.encode(UNICODE_ENCODING)
        self.encodeSized(mark, bytes, buffer, state)

    def encodeNone(self, mark, val, buffer, state):
        buffer.write(NONE_MARK)

    def encodeBool(self, mark, val, buffer, state):
        buffer.write(TRUE_MARK if val else FALSE_MARK)

    def encodeMap(self, mark, map, buffer, state):
        self.encodeCollection(mark, map.iteritems(), buffer, state)

    def encodeCollection(self, mark, collection, buffer, state):
        ## still too slow, but I can't figure out how to make it faster
        ## other than passing around an extra accumulator variable
        buffer.write(mark)
        for item in collection:
            self.encodeAny(item, buffer, state)
        buffer.write(UNSIZED_ENDING)

    def encodeUnsized(self, mark, val, buffer, state):
        buffer.write(mark)
        buffer.write(builtin_str(val))
        buffer.write(UNSIZED_ENDING)

    def encodeSized(self, mark, val, buffer, state):
        buffer.write(mark)
        buffer.write(builtin_str(len(val)))
        buffer.write(SIZED_DELIMETER)
        buffer.write(val)

    ##### Derialize/Decode #####

    def deserialize(self, bytes):
        state = Referer()
        offset = 0
        deserialized, offset, state = self.deserializeOne(bytes, offset, state)
        #left = bytes[offset:] #peek(offset, bytes)
        #if left != "":
        #    raise Exception("Stream was not cleaned up by decoder: %s" % (left,))
        return deserialized

    def deserializeAll(self, bytes):
        state  = Referer()
        offset = 0
        length = len(bytes)
        while offset < length:
            try:
                deserialized, offset, state = self.deserializeOne(bytes, offset, state)
                yield deserialized
            except:
                print (offset, length)
                raise
 
    def deserializeOne(self, bytes, offset, state):
        try:
            deserialized, offset = self.decodeAny(bytes, offset, state)

            return (deserialized, offset, state)
        except (IndexError, ValueError): # could happen anywhere we do a buffer read
            raise Exception("Invalid format")

    def decodeAny(self, bytes, offset, state):
        given_mark, offset = self.read(offset, bytes, 1)

        try:
            (type, decode) = self.typeAndDecoderByMark[given_mark]
        except KeyError:
            raise Exception("Cannot decode values of type %s" % given_mark)
        else:
            return decode(self, type, bytes, offset, state)

    def decodeUnicode(self, type, bytes, offset, state):
        val, offset = self.decodeSized(type, bytes, offset, state)
        #return text(val), offset
        return val.decode(UNICODE_ENCODING), offset

    def decodeNone(self, type, bytes, offset, state):
        return None, offset

    def decodeTrue(self, type, bytes, offset, state):
        return True, offset

    def decodeFalse(self, type, bytes, offset, state):
        return False, offset

    def decodeNone(self, type, bytes, offset, state):
        return None, offset

    def decodeBool(self, type, bytes, offset, state):
        val, offset = self.decodeUnsized(int, bytes, offset, state)
        return bool(val), offset

    def decodeMap(self, type, bytes, offset, state):
        return self.decodeCollection(type, bytes, offset, state)

    def decodeUnsized(self, type, bytes, offset, state):
        bytes, offset = self.readUntil(offset, bytes, UNSIZED_ENDING)
        offset        = offset + UNSIZED_ENDING_SIZE
        return type(bytes), offset

    def decodeSized(self, type, bytes, offset, state):
        size_bytes, offset = self.readUntil(offset, bytes, SIZED_DELIMETER)
        offset             = offset + SIZED_DELIMETER_SIZE #skip(offset, SIZED_DELIMETER_SIZE)
        size               = int(size_bytes)
        val, offset        = self.read(offset, bytes, size)
        return val, offset
        
    def decodeCollection(self, type, bytes, offset, state):
        decode    = self.decodeAny
        values    = []
        while bytes[offset] != UNSIZED_ENDING:
            val, offset = decode(bytes, offset, state)
            values.append(val)

        if type != list:
            values = type(values)

        return values, offset + UNSIZED_ENDING_SIZE

    def decodeSimplifyable(self, type, bytes, offset, state):
        tag,    offset = self.decodeAny(bytes, offset, state)
        values, offset = self.decodeCollection(list, bytes, offset, state)
        obj            = self.objectFromTagAndValues(tag, values)
        return obj, offset

    def objectFromTagAndValues(self, tag, values):
        cls = self.namedClasses.classFromName(tag)
        if tag is None or cls is None:
            raise Exception("Cannot deserialize value with tag %r" % tag)
        else:
            return cls.fromValues(values)

    @classmethod
    def read(cls, offset, buffer, count):
        end = offset + count
        return buffer[offset : end], end
