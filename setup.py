from setuptools import setup, find_packages

setup(
    name="stopat30m",
    version="1.0.0",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "pyqlib>=0.9.0",
        "lightgbm>=4.0.0",
        "xgboost>=2.0.0",
        "pandas>=2.0.0",
        "numpy>=2.0,<2.5",
        "scipy>=1.11.0",
        "scikit-learn>=1.3.0",
        "matplotlib>=3.7.0",
        "pyyaml>=6.0.0",
        "loguru>=0.7.0",
        "click>=8.1.0",
        "joblib>=1.3.0",
        "streamlit>=1.28.0",
        "requests>=2.31.0",
        "akshare>=1.14.0",
        "baostock>=0.8.8",
    ],
    extras_require={
        "trading": ["vnpy>=3.9.0", "vnpy-xtp>=2.2.0", "redis>=5.0.0", "sqlalchemy>=2.0.0", "pymysql>=1.1.0"],
        "torch": ["torch>=2.0.0"],
        "full": ["torch>=2.0.0", "vnpy>=3.9.0", "vnpy-xtp>=2.2.0", "redis>=5.0.0", "tushare>=1.4.0",
                  "sqlalchemy>=2.0.0", "pymysql>=1.1.0", "ta-lib>=0.4.28", "seaborn>=0.12.0", "plotly>=5.18.0"],
    },
    entry_points={
        "console_scripts": [
            "stopat30m=main:cli",
        ],
    },
)
