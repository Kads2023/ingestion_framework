from setuptools import setup, find_packages


setup(
    name="edap_common",
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