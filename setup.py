from setuptools import setup, find_packages

setup(
    name="piv",
    version="0.0.1",
    author="Luisa Carvajal",
    author_email="",
    description="",
    py_modules=["actividad_1","actividad_2"],
    install_requires=[
        "pandas==2.2.3",
        "openpyxl",
        "requests==2.32.3",
        "yfinance==0.2.59"
    ]
) 