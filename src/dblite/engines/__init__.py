"""
Engine modules are expected to have:
```
def autodetect(opts):
    '''
    Returns true if input is recognizable as connection options for this engine.
    @param   opts  connection options, as string or dict
    '''

def register_adapter(transformer, typeclasses):
    '''Registers function to auto-adapt given Python types for engine in query parameters.'''

def register_converter(transformer, typenames):
    '''Registers function to auto-convert given database types to Python in query results.'''

class Database(dblite.Database)

class Transaction(dblite.Transaction)
```
------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     18.11.2022
@modified    18.11.2022
------------------------------------------------------------------------------
"""
