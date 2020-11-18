import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="simpleBDB",
    version="1.0.3",
    author="Tristan Miller",
    author_email="Tristan.Miller@nau.edu",
    description="A simple wrapper for bsddb3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/deltarod/simpleBDB/",
    install_requires=['bsddb3'],
    packages=['simpleBDB'],
    extras_require={
        'test': ['pytest']
    },
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
    ],
    python_requires='>=3.6',
)
