# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import inspect
import os
import sys

sys.path.insert(0, os.path.abspath('../etdmap'))

project = 'etdmap - "Energietransitie Dataset" mapping package'
copyright = '2025, Nicolas Dickinson, Marten Witkamp, Petra Izeboud'
author = 'Nicolas Dickinson, Marten Witkamp, Petra Izeboud'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_parser',
    'sphinx.ext.napoleon',
    'sphinx.ext.autodoc',
    'sphinx.ext.autodoc.typehints',
    'sphinx.ext.viewcode',
    'sphinx.ext.linkcode',
    'numpydoc',
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

numpydoc_show_class_members = True

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

#html_theme = 'alabaster'
html_theme = "sphinx_rtd_theme"
html_static_path = ['_static']

# Linking to source code configuration
def linkcode_resolve(domain, info):
    if domain != 'py':
        return None
    if not info['module']:
        return None

    modname = info['module']
    fullname = info['fullname']

    submod = sys.modules.get(modname)
    if submod is None:
        return None

    obj = submod
    for part in fullname.split('.'):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            return None

    try:
        fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError:
        fn = None
    if not fn:
        return None

    try:
        source, lineno = inspect.getsourcelines(inspect.unwrap(obj))
    except OSError:
        lineno = ""

    fn = os.path.relpath(fn, start=os.path.abspath('../'))  # Adjust this path if needed

    return f"https://github.com/Stroomversnelling/etdmap/blob/main/{fn}#L{lineno}"


linkcode_options = {
    'resolve': linkcode_resolve,
}
