from setuptools import setup

setup(
    name='meesee',
    author='Melvin Bijman',
    author_email='bijman.m.m@gmail.com',
    version='0.1',
    py_modules=['meesee'],
    install_requires=['redis==2.10.6'],
    license='MIT',

    url='https://github.com/Attumm/meesee',

    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
