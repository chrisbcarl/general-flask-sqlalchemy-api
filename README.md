# generic-sql-api
a working example of a semi-decent [Flask](https://flask.palletsprojects.com/en/1.1.x/) app running [sqlalchemy](https://www.sqlalchemy.org/) to create a `CRUD` `API` for all tables and basic methods.


# Installation
```bash
touch generic-sql-api.conf
# activate a venv if you'd like
python -m pip install -r requirements.txt
python generic-sql-api.py --help  # displays the args and an example config format
```

# Running
```bash
python generic-sql-api.py
```

# Debugging
* with [vscode](https://code.visualstudio.com/) for example:
    ```json
    {
        "name": "generic-sql-api --debug",
        "type": "python",
        "request": "launch",
        "program": "${workspaceFolder}/generic-sql-api.py",
        "console": "integratedTerminal",
        "args": [
            "--debug"
        ]
    }
    ```


# What's included
* a single generic endpoint that demonstrates and responds correctly to classic `REST` idioms
* lots of code snippets, including the config
* a few examples of modifying a `sqlalchemy` query over time
* a decent pipeline of common Flask pitfalls
* fairly mid-level manipulation of orm objects and the session
* thread-safe `Flask` / `sqlalchemy` compatibility through the `scoped_session` idiom


# What's missing
* flask
  * a good WSGI server like [waitress](https://docs.pylonsproject.org/projects/waitress/en/latest/)
  * authorization from users
  * multiple databases on the same server/instance
  * handle other sql dialects, currently only `mssql`
  * blueprints
  * a home page, lol
  * a docs page, lol
  * swagger definition
  * etc.
* sqlalchemy
  * auto joined tables
  * handle primary keys that are not `id`
  * super-duper sanitize inputs
  * etc.
* computer science
  * caching like a [redis](https://redis.io/) or [beanstalk](https://aws.amazon.com/elasticbeanstalk/)
  * it's pretty much flying by the seat of its pants
  * good memory management
  * some more command line arguments like changing the logpath or the config path
  * SSL (though most likely that would be handled by a webserver like [nginx](http://nginx.org/))


### Bragging
* started at 2021-02-27 21:53
* finished metadata, permissions, all of resource and resource/id by 2021-02-28 01:27
