from builtins import __import__


def mock_import(exclude):
    def inner(name, globals, locals, fromlist, level):
        if name == exclude:
            raise ModuleNotFoundError()
        return __import__(name, globals, locals, fromlist, level)

    return inner
