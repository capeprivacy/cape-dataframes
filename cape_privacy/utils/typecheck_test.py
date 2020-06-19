from cape_privacy.utils import typecheck


def _make_args_and_types():
    string = "hi"
    integer = 4
    flt = 2.0
    lst = [string, integer, flt]
    tpl = (string, integer, float)
    args_list = (string, integer, flt, lst, tpl, None)
    types_list = (str, int, float, list, tuple, type(None))
    return args_list, types_list


def test_typecheck_args():
    args_list, types_list = _make_args_and_types()
    # check passing
    for a, t in zip(args_list, types_list):
        typecheck.check_arg(a, t)
    # check failure
    for a, t in zip(args_list, types_list[::-1]):
        try:
            typecheck.check_arg(a, t)
            raise AssertionError
        except ValueError:
            pass


def test_typecheck_more_types_passes():
    args_list, types_list = _make_args_and_types()
    args = args_list[:3]
    types = types_list[:3]
    for arg in args:
        typecheck.check_arg(arg, types)


def test_typecheck_more_types_fails():
    args_list, types_list = _make_args_and_types()
    arg = args_list[-1]
    types = types_list[:3]
    try:
        typecheck.check_arg(arg, types)
        raise AssertionError
    except ValueError:
        pass
