import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name='kdap',
    version='0.1.28',
    author="Amit Arjun Verma",
    author_email="mt4descentis@gmail.com",
    description="KDAP is a package to analyze knowledge data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://kdap.github.io/",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=['requests==2.21.0',
                      'internetarchive==1.8.5',
                      'numpy',
                      'wikipedia',
                      'pycountry',
                      'psutil>=5.6.6',
                      'mwparserfromhell==0.5.4',
                      'nltk==3.4.5',
                      'xmltodict',
                      'matplotlib',
                      'prettytable',
                      'bx-python',
                      'pyunpack',
                      'mwviews',
                      'patool'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)
