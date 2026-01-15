import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="restate_client",
    version="0.1.0",
    author="Jassim Abdul Latheef",
    author_email="jassim@glance.care",
    description="Shared Python library for restate client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/groacare/glance",
    project_urls={
        "Bug Tracker": "https://glance-care.atlassian.net/jira/software/c/projects/PD"
    },
    license="Other/Proprietary License",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.11",
    install_requires=["requests", "restate-sdk>=0.5.1", "aiohttp==3.13.2"],
)
