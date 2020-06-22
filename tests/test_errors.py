from frictionless import errors, exceptions


# From exception


def test_error_from_exception():
    exception = exceptions.SourceError('message')
    error = errors.Error.from_exception(exception)
    assert error['code'] == 'source-error'
