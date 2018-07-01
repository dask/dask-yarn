from setuptools import setup

setup(name='dask-yarn',
      version='0.0.1',
      license='BSD',
      description='Deploy dask clusters on Apache YARN',
      packages=['dask_yarn', 'dask_yarn.cli'],
      include_package_data=True,
      install_requires=['distributed', 'skein'],
      entry_points='''
        [console_scripts]
        dask-yarn-worker=dask_yarn.cli.dask_yarn_worker:main
        dask-yarn-scheduler=dask_yarn.cli.dask_yarn_scheduler:main
      ''',
      zip_safe=False)
