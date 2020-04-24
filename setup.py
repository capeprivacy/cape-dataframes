"""Installing with setuptools."""
import setuptools

setuptools.setup(
    name="cape",
    version="0.0.1",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    license="Apache License 2.0",
    url="https://github.com/capeprivacy/cape",
    description="Cape manages secure access to all of your data.",
    long_description_content_type="text/markdown",
    author="Cape Privacy",
    author_email="contact@capeprivacy.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 2 - Alpha",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Security :: Cryptography",
    ]
)