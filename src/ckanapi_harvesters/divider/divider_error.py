"""
Module for custom exceptions related to calculator operations.

This module defines custom exceptions used in calculator operations,
including a base exception class and a specific exception for division by zero errors.

Classes
-------
CalculatorError
    Base class for exceptions in calculator operations.
CantDivideByZeroError
    Exception raised when an attempt is made to divide by zero.

Exceptions
----------
CalculatorError
    Base class for exceptions in the calculator domain.
CantDivideByZeroError
    Raised specifically for division by zero errors.
"""


class CalculatorError(Exception):
    """
    Base class for exceptions in calculator operations.

    This class is intended to be used as a base class for other calculator-related
    exceptions. It inherits from the built-in Exception class and allows for custom
    exception handling in the calculator domain.

    Parameters
    ----------
    args : tuple
        Variable length argument list passed to the base Exception class.
    """

    def __init__(self, *args):
        super().__init__(args)


class CantDivideByZeroError(CalculatorError):
    """
    Exception raised when an attempt is made to divide by zero.

    This exception is a specific subclass of CalculatorError and is intended to be
    used when a division by zero error occurs. It provides a custom error message
    indicating that division by zero is not allowed.

    Parameters
    ----------
    None

    Notes
    -----
    The default message for this exception is "tu ne peux pas diviser par zéro".
    """

    def __init__(self):
        super().__init__('tu ne peux pas diviser par zéro')
