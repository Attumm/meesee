import io

from setuptools import setup

with io.open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='meesee',
    author='Melvin Bijman',
    author_email='bijman.m.m@gmail.com',

    description='Task queue, Long lived workers process parallelization, with Redis as backend',
    long_description=long_description,
    long_description_content_type='text/markdown',

    version='1.0.0',
    py_modules=['meesee'],
    install_requires=['redis==3.5.3'],
    python_requires='>3.5',
    license='MIT',

    url='https://github.com/Attumm/meesee',

    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
