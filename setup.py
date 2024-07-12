# setup.py
from setuptools import setup, find_packages

setup(
    name='eon-task-project',
    version='0.1.0',
    packages=find_packages(include=['your_package', 'your_package.*']),
    py_modules=['script_in_root', 'another_script_in_root'],
    install_requires=[
        # List your project's dependencies here
        'numpy',
        'pandas',
        'paramiko',
        'boto3',
        'pytest'
    ],
    entry_points={
        'console_scripts': [
            'main_command=raw_data_files_s3_uploader:main',
        ],
    },
    author='Emad',
    author_email='syedemad11@gmail.com',
    description='A brief description of your project',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/syedemz/eon-task.git',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12',
)
