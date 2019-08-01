from setuptools import setup
import versioneer

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

with open('README.rst') as f:
    long_description = f.read()

setup(name='dask-yarn',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      license='BSD',
      description='Deploy dask clusters on Apache YARN',
      long_description=long_description,
      packages=['dask_yarn'],
      include_package_data=True,
      install_requires=install_requires,
      python_requires=">=3.5",
      entry_points='''
        [console_scripts]
        dask-yarn=dask_yarn.cli:main
      ''',
      zip_safe=False)
