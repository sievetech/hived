from six import PY3

if PY3:
    import sys
    from unittest import mock
    sys.modules['mock'] = mock
