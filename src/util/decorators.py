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
