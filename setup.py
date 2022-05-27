import setuptools
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='standard_helpers',
    version='0.0.1',
    author='Robb Dunlap',
    author_email='robb@altana.ai',
    description='testing pip installation of github packages',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/robb-altoid/standard_helpers',
    project_urls = {
        "Bug Tracker": "https://github.com/robb-altoid/standard_helpers/issues"
    },
    license='MIT',
    packages=['standard_helpers'],
    install_requires=['typing','pyspark','pandas'],
)