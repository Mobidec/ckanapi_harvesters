"""
Module for division operations with custom exceptions.

This module provides a function for performing division
and raises a custom exception when attempting to divide by zero.

Functions
---------
divide(a, b)
    Divide two numbers, raising a custom exception if the divisor is zero.

Exceptions
----------
CantDivideByZeroError
    Raised when an attempt is made to divide by zero.
"""

from .divider_error import CantDivideByZeroError


def divide(a, b):
    """
    Divide two numbers, raising a custom exception if the divisor is zero.

    Parameters
    ----------
    a : float
        The dividend.
    b : float
        The divisor.

    Returns
    -------
    float
        The result of the division.

    Raises
    ------
    CantDivideByZeroError
        If the divisor (b) is zero.

    Examples
    --------
    >>> divide(10, 2)
    5.0
    >>> divide(10, 0)
    Traceback (most recent call last):
        ...
    CantDivideByZeroError
    """
    if b == 0:
        raise CantDivideByZeroError()
    return a / b
