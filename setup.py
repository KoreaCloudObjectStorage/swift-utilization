from setuptools import setup

setup(name='swiftutilization',
      version='0.0.1',
      description='Middleware to save transfer utilization in swift',
      license='Apache License (2.0)',
      author='a2company',
      author_email='admin@a2company.co.kr',
      packages=['swiftutilization', 'swiftutilization/middleware',
                'swiftutilization/daemon'],
      install_requires=['swift >= 1.13.0'],
      entry_points={'paste.filter_factory':
                    ['swiftutilization='
                     'swiftutilization.middleware.utilization:filter_factory']},
      scripts={'bin/swift-utilization-aggregator'})

