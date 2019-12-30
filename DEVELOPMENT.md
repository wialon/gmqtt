# Development

If you want to contribute to `gmqtt` or work on the code it's recommended
that you use a Python virtual environment (`venv`).

After you have create a fork of the original repository, clone it to your
local system. 

```bash
git clone git@github.com:[YOUR_GITHUB_USERNAME]/gmqtt.git
cd gmqtt
python3 -m venv .
source bin/activate
python3 setup.py develop
```

Also, add the upstream repository and rebase from time to time to stay
up-to-date with the on-going development.

```bash
git remote add upstream git@github.com:wialon/gmqtt.git
git pull --rebase upstream master
```

For a new feature or change, create a new branch locally and then you are 
finish open a Pull Request.

## Run tests

First install the additional dependencies which are required to run the tests.

```bash
pip3 install .[test]
```

The unit tests require that you have a [flespi.io account](https://flespi.io/).
You will need the token from [https://flespi.io/#/panel/list/tokens](https://flespi.io/#/panel/list/tokens)
which then is made available as an environment variable.

```bash
export USERNAME=YOUR_FLESPI_IO_TOKEN
```

Now, you can run the tests locally.

```bash
pytest-3 tests
```
