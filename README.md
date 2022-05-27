# standard_helpers
standard tools to make databricks analytic workflow better

- you must install databricks-sql-connector and deactivate pyarrow in your databricks environment before importing this module. use the following commands in a cell above your import statments:
```python
spark.conf.set("spark.sql.execution.arrow.enabled", "false")

try:
    __import__("databricks-sql-connector")
except ImportError:
    ! pip install databricks-sql-connector
```
- pip install the package with:
```python
pip install git+https://github.com/robb-altoid/standard_helpers
```
- import the quick_query module with:
```python
from standard_helpers import quick_query as qq
```

for some reason (in my environment), converting data from a spark df to a pandas df won't work if spark arrow has been enabled.
I got instructions for enabling/disabling spark arrow from [here](https://george-jen.gitbook.io/data-science-and-apache-spark/enabling-for-conversion-to-from-pandas)

thanks to https://github.com/mike-huls for his article [Medium article](https://towardsdatascience.com/create-your-custom-python-package-that-you-can-pip-install-from-your-git-repository-f90465867893) on creating a PIP installable package on GitHub