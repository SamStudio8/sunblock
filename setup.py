from setuptools import setup, find_packages

setup(
    name='sunblock',
    version='0.0.0',

    author="Sam Nicholls",
    author_email="sam@samnicholls.net",

    description="Do not stare directly into the Sun Grid Engine.",
    long_description=open('README.md').read(),
    url='https://github.com/samstudio8/sunblock',

    license='MIT License',

    packages=find_packages(),

    package_data={
        'sunblock': ['templates/*.json']
    },

    entry_points={
        'console_scripts': [
            'sunblock = sunblock:cli',
        ]
    },

    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ]
)

