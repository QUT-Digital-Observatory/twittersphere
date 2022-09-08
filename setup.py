import setuptools

install_requires = ["click>=8.0.0", "regex>=2021.3.17", "glom>=22.1.0"]

extras_require = {"development": ["nox", "pytest", "pip-tools"]}

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="twittersphere",
    author="QUT Digital Observatory",
    author_email="digitalobservatory@qut.edu.au",
    description="Tools for content based filtering of tweets and Twitter accounts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/QUT-Digital-Observatory/twittersphere",
    license="MIT",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Sociology",
    ],
    keywords="social_science social_media_analysis",
    python_requires=">=3.9",
    setup_requires=["setuptools_scm"],
    use_scm_version=True,
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={"console_scripts": ["twittersphere=twittersphere:cli.twittersphere"]},
)
