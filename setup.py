import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Tagdir",
    version="0.0.1",
    author="Yuu Igarashi",
    author_email="yuu.igarashi.service@gmail.com",
    description="Tag filesystem",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yu-i9/Tagdir",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Topic :: System :: Filesystems"
    ],
)
