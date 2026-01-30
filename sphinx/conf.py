"""
Sphinx configuration file for the documentation of the project.

This file contains the configuration settings for Sphinx to generate
documentation for the project. It includes paths, project metadata,
extensions, and options for documentation output.

Configuration Options:
----------------------
- `sys.path.insert(0, os.path.abspath("../src"))`: Adds the source directory to the module search path.
- `project`: The name of the project.
- `author`: The name of the author(s) of the project.
- `release`: The release version of the project.

Extensions:
-----------
- `sphinx_design`: Adds design-related functionality.
- `myst_parser`: Enables the MyST markdown parser.  # included with myst_nb
- `sphinx.ext.duration`: Measures the time taken to build the documentation.
- `sphinx.ext.doctest`: Tests that docstrings are valid Python code.
- `sphinx.ext.autodoc`: Automatically documents Python modules.
- `sphinx.ext.autosummary`: Generates summary tables of documented modules.
- `sphinx_autopackagesummary`: Automatically generates package summaries.
- `sphinx.ext.napoleon`: Supports Google and NumPy style docstrings.
- `myst_nb`: Supports including Jupyter notebooks.  [doc here](https://docs.readthedocs.com/platform/latest/guides/jupyter.html)

Napoleon Settings:
------------------
- `napoleon_google_docstring`: Enables Google style docstrings.
- `napoleon_numpy_docstring`: Enables NumPy style docstrings.
- `napoleon_include_init_with_doc`: Includes __init__ method docstrings.
- `napoleon_include_private_with_doc`: Includes private members in the documentation.
- `napoleon_include_special_with_doc`: Includes special methods in the documentation.
- `napoleon_use_admonition_for_examples`: Uses admonitions for examples.
- `napoleon_use_admonition_for_notes`: Uses admonitions for notes.
- `napoleon_use_admonition_for_references`: Uses admonitions for references.
- `napoleon_use_ivar`: Uses the "ivar" style for instance variables.
- `napoleon_use_param`: Uses the "param" style for parameters.
- `napoleon_use_rtype`: Uses the "rtype" style for return types.
- `napoleon_use_keyword`: Uses the "keyword" style for keyword arguments.
- `napoleon_custom_sections`: Custom sections for docstrings (currently set to None).

Internationalization:
----------------------
- `language`: Sets the language for the documentation output (set back to English).

HTML Output Options:
---------------------
- `html_theme`: Specifies the theme for HTML output (set to "sphinx_rtd_theme").
- `html_static_path`: Path to static files (set to "_static").
- `html_css_files`: List of custom CSS files (set to "custom.css").
"""

import os
import sys

sys.path.insert(0, os.path.abspath('../src'))

project = 'dev_update_sphinx'
author = 'author'
release = '0.0.0'

extensions = [
    'sphinx_design',
    # 'myst_parser',  # included with myst_nb
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx_autopackagesummary',
    'sphinx.ext.napoleon',
    'sphinx_rtd_theme',
    'myst_nb',
]

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_use_keyword = True
napoleon_custom_sections = None

nb_execution_mode = "off"  # https://myst-nb.readthedocs.io/en/latest/computation/execute.html

# autosummary_generate = True  # Turn on sphinx.ext.autosummary

language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_css_files = [
    'custom.css',
]
