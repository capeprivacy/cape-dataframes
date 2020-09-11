"""Installing with setuptools."""
import setuptools


with open("README.md", "r", encoding="utf8") as fh:
  long_description = fh.read()

setuptools.setup(
    name="cape-privacy",
    version="0.3.0",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    license="Apache License 2.0",
    url="https://github.com/capeprivacy/cape-python",
    description="Cape manages secure access to all of your data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Cape Privacy",
    author_email="contact@capeprivacy.com",
    install_requires=[
        "requests==2.23.0",
        "pandas==1.0.3",
        "numpy==1.18.1",
        "pyyaml==5.3.1",
        "validators==0.18.0",
        "pycryptodome==3.9.8",
    ],
    extras_require={
        "spark": ["pyspark >=2.4", "pyarrow >=0.15.1"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Security :: Cryptography",
    ]
)