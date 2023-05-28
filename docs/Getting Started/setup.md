# Setup
## Requirements
- A Windows, Linux or Mac operating system (note that you may need to adjust python to python3, etc. when installing on Mac or Linux)
- [Python 3.10](https://www.python.org/downloads/release/python-31011/) since Python 3.11 isn't fully supported by all ML libraries
- [Docker](https://www.docker.com/products/docker-desktop/) (If you intend to run the docs locally)
- Pip (Installed with Python3)
- [Git](https://git-scm.com/downloads)
## Install
1. Install the requirements listed above.
2. clone the [github repo](https://github.com/aaron777collins/ConnectedDrivingPipelineV4) by running the following in your git bash terminal (note that on the super computer you should do this within the projects directory):
``` bash
git clone https://github.com/aaron777collins/ConnectedDrivingPipelineV4.git
```
3. cd into the folder by typing:
``` bash
cd ConnectedDrivingPipelineV4
```
*Note that all your future commands will be expected to start from this directory.*
4. Create a folder called `data`
``` bash
mkdir data
```
5. Download the data from [here](https://data.transportation.gov/api/views/9k4m-a3jc/rows.csv?accessType=DOWNLOAD) and name it `data.csv` within the `data` folder.

    **Optionally**: You can download wget.exe from [https://eternallybored.org/misc/wget/](https://eternallybored.org/misc/wget/) and put it in your git bash directory (The default Windows install directory is `C:\Program Files\Git\mingw64\bin`).
    Next, run the following in git bash (within the project directory) to download the file:
    ``` bash
    while [ 1 ]; do
        wget --retry-connrefused --retry-on-http-error=500 --waitretry=1 --read-timeout=20 --timeout=15 -t 0 --continue -O data/data.csv https://data.transportation.gov/api/views/9k4m-a3jc/rows.csv?accessType=DOWNLOAD
        if [ $? = 0 ]; then break; fi; # check return value, break if successful (0)
        echo `error downloading. Trying again!`
        sleep 1s;
    done;
    ```

6. Create a virtual environment to install the required modules
    ``` bash
    python -m venv venv
    ```
    and then activate your instance by running
    ``` bash
    ./venv/Scripts/activate
    ```
    on Windows. For other OS' consult the python docs [here](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)

    *Make sure to always activate your venv before running anything in this pipeline*

7. Install the required modules (note that on the super computer you can use --no-index to install from the local cache)

    ``` bash
    pip install -r requirements.txt
    ```

**You're Finished Installing!**
Head over to the [Development](development.md) page to learn how to make your first pipeline user.
