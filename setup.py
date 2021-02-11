from setuptools import setup

setup(
    name='meesee',
    author='Melvin Bijman',
    author_email='bijman.m.m@gmail.com',
    version='0.9.2',
    py_modules=['meesee'],
    install_requires=['redis==2.10.6'],
    license='MIT',

    url='https://github.com/Attumm/meesee',

    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
