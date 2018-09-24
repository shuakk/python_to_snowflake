# python_to_snowflake

This is the test repo to load some python script for other users to download and run in their virtual environment.

The function included here is to type username and password for snowflake and execute a sql query, the result will be returned in terminal or local file.

Starter Code:
1. git clone the repo
2. which python3: Give you the dir of your python3
3. virtualenv --python=/python3 dir.python3 venv
   (Need to install virtual Envirionment first, pip3 install virtualenv)
4. cd /path to git clone folder
5. . venv/bin/activate
6. pip install -r conf/requirements.txt
7. python src/file.py
8. After script, deactivate to deactivate the venv 
