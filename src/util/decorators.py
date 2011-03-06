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

# This file makes decorators easier, and has some handy decorators as
# well.

## Use as follows:
#
# @decorator
# def log_error(func, *args, **kargs):
#    try:
#        return func(*args, **kargs)
#    except:
#        log()
#
# @log_error
# def blowup():
#    raise Exception("blow up")
#
# blowup() # this gets caught and calls log()
#
def decorator(decorator):
    def new_decorator(func):
        def decorated(*args, **kargs):
            return decorator(func, *args, **kargs)

        decorated.__name__ = func.__name__
        return decorated
    return new_decorator

## Use as follows:
#
# @decoratorN(1)
# def log_error(log_message, func, *args, **kargs):
#    try:
#        return func(*args, **kargs)
#    except:
#        log(log_message, ...)
# 
# @log_error("explosion")
# def blowup():
#    raise Exception("blow up")
#
# blowup()  # this gets caught and calls log("explosion")
#
def decorator_with_args(decorator_nargs):
    def decorator(decorator):
        def decorator_factory(*decorator_args):
            assert len(decorator_args) == decorator_nargs, "{} expects {} args".format(
                decorator_.__name__, decorator_nargs)
            def new_decorator(func):
                def decorated(*args, **kargs):
                    full_args = decorator_args + (func,) + args
                    return decorator(*full_args, **kargs)

                decorated.__name__ = func.__name__
                return decorated
            return new_decorator
        return decorator_factory
    return decorator

# Use as follows:
#   @into(list)
#   def get_values():
#      yield "foo"
#   
#   values = get_values()  # values is now ["foo"]
@decorator_with_args(1)
def into(container, func, *args, **kargs):
    return container(func(*args, **kargs))

# If you have an enum of types and a record where the first value is
# the type, this will let you say Record.type1(arg2, arg3).  It sounds
# tricky, but it's really handy.
def type_constructors(types):
    def add_type_constructor(cls, type):
        @classmethod
        def type_constructor(cls, *args):
            return cls(type, *args)

        setattr(cls, type.name, type_constructor)

    def add_type_constructors(cls):
        for type in types:
            add_type_constructor(cls, type)
        return cls

    return add_type_constructors
