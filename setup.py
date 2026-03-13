from setuptools import setup, find_packages

setup(
    name="stopat30m",
    version="1.0.0",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "pyqlib>=0.9.0",
        "lightgbm>=4.0.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "scikit-learn>=1.3.0",
        "pyyaml>=6.0.0",
        "loguru>=0.7.0",
        "click>=8.1.0",
        "streamlit>=1.28.0",
        "redis>=5.0.0",
    ],
    entry_points={
        "console_scripts": [
            "stopat30m=main:cli",
        ],
    },
)
