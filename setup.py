from setuptools import setup

setup(name='dask-yarn',
      version='0.0.1',
      license='BSD',
      description='Package conda environments for redistribution',
      packages=['dask_yarn', 'dask_yarn.cli'],
      entry_points='''
        [console_scripts]
        dask-yarn-worker=dask_yarn.cli.dask_yarn_worker:main
        dask-yarn-scheduler=dask_yarn.cli.dask_yarn_scheduler:main
      ''',
      zip_safe=False)
