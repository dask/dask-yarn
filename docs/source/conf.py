# Project details
project = 'Dask Yarn'
copyright = '2018, dask-yarn authors'
author = 'dask-yarn authors'
version = release = ''

# Source details
source_suffix = '.rst'
exclude_patterns = []
master_doc = 'index'
language = None

# Extensions
extensions = [
    'sphinx.ext.autodoc',
    'numpydoc',
    'sphinxcontrib.autoprogram'
]
numpydoc_show_class_members = False

# Theming
html_theme = 'dask_sphinx_theme'
templates_path = ['_templates']
html_static_path = ['_static']
htmlhelp_basename = 'dask-yarndoc'
pygments_style = 'default'
