from setuptools import setup, find_packages

setup(
    name='looker_ingestion',
    version='1.0.1',
    description='Extracts adhoc queries from the Looker API to S3',
    packages=find_packages(),
    entry_points={'console_scripts': [
                'extract_looker_metadata = looker_ingestion.sync_data:main'
            ]
    },
    install_requires=[
        'boto3',
        'looker_sdk'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'moto'],
        classifiers= [
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3"
    ]
)