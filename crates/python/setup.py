from setuptools import setup, find_packages
from setuptools_rust import Binding, PyBin, RustExtension

setup(
    name="deltalakedb",
    version="0.1.0",
    description="Python bindings for SQL-Backed Delta Lake metadata",
    long_description=open("README.md").read() if open("README.md") else "",
    long_description_content_type="text/markdown",
    author="DeltaLakeDB Authors",
    author_email="authors@deltalakedb.org",
    url="https://github.com/your-org/deltalakeDB",
    license="MIT OR Apache-2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Rust",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "typing-extensions>=3.7.4",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-asyncio>=0.18",
            "mypy>=0.900",
            "black>=21.0",
            "isort>=5.0",
        ],
    },
    rust_extensions=[
        RustExtension(
            "deltalakedb._deltalakedb",
            binding=Binding.PyO3,
            py_limited_api=True,
            path="Cargo.toml",
            args=["--release"],
        ),
    ],
    packages=find_packages(exclude=["tests", "tests.*"]),
    zip_safe=False,
)