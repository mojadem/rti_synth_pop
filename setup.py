import os

from setuptools import setup, find_packages


def readme() -> str:
    """Utility function to read the README.md.

    Used for the `long_description`. It's nice, because now
    1) we have a top level README file and
    2) it's easier to type in the README file than to put a raw string in below.

    Args:
        nothing

    Returns:
        String of readed README.md file.
    """
    return open(os.path.join(os.path.dirname(__file__), "README.md")).read()


setup(
    name="rti_synth_pop",
    version="0.1.0",
    author="Nick Kruskamp, Caroline Kery, James Rineer (RTI International)",
    author_email="nfkruskamp@gmail.com, ckery@rti.org, jrin@rti.org",
    description="rti_synth_pop synthetic population generator",
    python_requires=">=3.11",
    license="CC BY-NC-SA 4.0",
    url="https://github.com/RTIInternational/rti_synth_pop",
    packages=find_packages(),
    long_description=readme(),
)
