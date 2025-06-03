from setuptools import setup, find_packages


setup(
    name="edp_automation",
    version="0.0.1",
    author="ksaradha",
    description="a simple python wheel",
    packages=find_packages(),
    install_requires=[
        'setuptools'
    ]
)


# Y:

# cd \

# rm Y:\utils\wheel_files\*

# cd Y:\utils\common_utility\src\

# Y:\envs\myvenv\python.exe setup.py bdist_wheel --dist_dir Y:\utils\wheel_files\

# conda create --name trial_venv python=3.11.0 --no-default-packages

# conda create --prefix Y:\envs\trial_venv python=3.11.0 --no-default-packages -y

# conda activate Y:\envs\trial_venv

# conda install pyspark==3.5.4
# conda install azure-identity==1.19.0
# conda install azure-storage-file-datalake==12.17.0
# conda install great-expectations==1.4.2

# pytest Y:\<full_path>\tests\test_<file_name>.py