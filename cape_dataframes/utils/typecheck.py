def check_arg(arg, types):
    if not isinstance(arg, types):
        if not isinstance(types, (tuple, list)):
            types = (types,)
        raise ValueError("Expected one of {}, got {}.".format(types, type(arg)))
