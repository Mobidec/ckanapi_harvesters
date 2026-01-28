"""
Module for demonstrating a simple 'Hello World' function.

This module imports the hello_world function from the main module
and makes it available for public use.

Functions
---------
hello_world()
    Print 'Hello World!' to the console.
"""

from .main import hello_world

__all__ = ['hello_world']
