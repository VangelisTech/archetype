from setuptools import setup, find_packages

setup(
    name="vangelis-core",
    version="0.1.0",
    description="Core ECS and LanceDB/Daft integration for Vangelis",
    author="everettVT",
    author_email="everett@vangelis.tech",
    packages=find_packages(include=["core", "core.*"]),
    install_requires=[
        "daft",
        "pyarrow",
        "pandera"
    ],
    python_requires=">=3.10",
) 