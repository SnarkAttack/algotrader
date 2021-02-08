import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="algotrader",
    version="0.0.1",
    author="Patrick McQuay",
    author_email="patrick.mcquay@gmail.com",
    description="Algorithmic trader",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'cbpro',
        'python-dateutil'
    ],
    entry_points={
        'console_scripts': [
            'backtester = scripts.backtester:main',
            'cryptowatch = scripts.cryptowatch_websocket_service:main'
        ],
    },
    python_requires='>=3.6',
)