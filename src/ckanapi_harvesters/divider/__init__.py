"""
Module for division operations and custom exceptions.

This module provides functions and exceptions related to division operations.
It imports the `divide` function and the `CantDivideByZeroError` exception from
other modules and makes them available for use in this module.

Functions
---------
divide(a, b)
    Divide two numbers, raising a custom exception if the divisor is zero.

Exceptions
----------
CantDivideByZeroError
    Raised when an attempt is made to divide by zero.

Imports
--------
- divide: Function for performing division operations.
- CantDivideByZeroError: Exception raised for division by zero errors.
"""

from .divider import divide
from .divider_error import CantDivideByZeroError

__all__ = ['divide', 'CantDivideByZeroError']
