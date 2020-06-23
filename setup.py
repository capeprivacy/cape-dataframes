"""Installing with setuptools."""
import setuptools

setuptools.setup(
    name="cape-privacy",
    version="0.1.0rc0",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    license="Apache License 2.0",
    url="https://github.com/capeprivacy/cape-python",
    description="Cape manages secure access to all of your data.",
    long_description_content_type="text/markdown",
    author="Cape Privacy",
    author_email="contact@capeprivacy.com",
    install_requires=[
        "requests==2.23.0",
        "pandas==1.0.3",
        "numpy==1.18.1",
        "pyyaml==5.3.1",
        "validators==0.15.0",
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