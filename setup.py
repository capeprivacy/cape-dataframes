"""Installing with setuptools."""
import setuptools

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cape-dataframes",
    version="0.3.1",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    license="Apache License 2.0",
    url="https://github.com/capeprivacy/cape-dataframes",
    description="Cape manages secure access to all of your data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Cape Privacy",
    author_email="contact@capeprivacy.com",
    install_requires=[
        "pandas",
        "pycryptodome",
        "pyyaml",
        "requests",
        "rfc3339",
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
    ],
)
