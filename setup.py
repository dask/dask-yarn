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
      packages=['dask_yarn', 'dask_yarn.cli'],
      include_package_data=True,
      install_requires=install_requires,
      entry_points='''
        [console_scripts]
        dask-yarn-worker=dask_yarn.cli.dask_yarn_worker:main
        dask-yarn-scheduler=dask_yarn.cli.dask_yarn_scheduler:main
      ''',
      zip_safe=False)
