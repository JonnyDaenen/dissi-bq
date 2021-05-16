import setuptools

REQUIRED_PACKAGES = [
]


setuptools.setup(
    name='json2bq',
    version='0.0.1',
    description='JSON to BQ loader with built-in schema unification',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
