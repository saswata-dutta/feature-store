from setuptools import setup

setup(
    name="featurestore",
    version="0.1.3",
    packages=["featurestore/clients"],
    include_package_data=True,
    python_requires="~=3.5",
    install_requires=["boto3", "botocore", "pandas", "pytz", "pyspark"],
    description="A python sdk to create and upload Feature-Groups within AWS",
    url="https://github.com/saswata-dutta/aws-feature-store",
    author="Saswata Dutta",
    author_email="saswat.dutta@gmail.com",
)
