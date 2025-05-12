from setuptools import setup, find_packages

setup(
    name="archetype",
    version="0.1.0",
    description="Core ECS and LanceDB/Daft integration for Vangelis",
    author="everettVT",
    author_email="everett@vangelis.tech",
    packages=find_packages(include=["archetype", "archetype.*"]),
    install_requires=[
        "daft",
        "pyarrow",
        "pylance",
        "lancedb"
    ],
    python_requires=">=3.10",
) 