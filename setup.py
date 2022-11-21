# -*- coding: utf-8 -*-
"""
Setup.py for dblite.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     16.11.2022
@modified    16.11.2022
------------------------------------------------------------------------------
"""
import os
import re
import setuptools

PACKAGE = "dblite"


def readfile(path):
    """Returns contents of path, relative to current file."""
    root = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root, path)) as f: return f.read()

def get_description():
    """Returns package description from README."""
    LINK_RGX = r"\[([^\]]+)\]\(([^\)]+)\)"  # 1: content in [], 2: content in ()
    linkify = lambda s: "#" + re.sub(r"[^\w -]", "", s).lower().replace(" ", "-")
    # Unwrap local links like [Page link](#page-link) and [LICENSE.md](LICENSE.md)
    repl = lambda m: m.group(1 if m.group(2) in (m.group(1), linkify(m.group(1))) else 0)
    return re.sub(LINK_RGX, repl, readfile("README.md"))

def get_version():
    """Returns package current version number from source code."""
    VERSION_RGX = r'__version__\s*\=\s*\"*([^\n\"]+)'
    content = readfile(os.path.join("src", PACKAGE, "__init__.py"))
    match = re.search(VERSION_RGX, content)
    return match.group(1).strip() if match else None


setuptools.setup(
    name                 = PACKAGE,
    version              = get_version(),

    description          = "Simple query interface for SQL databases",
    url                  = "https://github.com/suurjaak/" + PACKAGE,
    author               = "Erki Suurjaak",
    author_email         = "erki@lap.ee",
    license              = "MIT",
    platforms            = ["any"],
    keywords             = "SQL SQLite Postgres psycopg2",
    python_requires      = ">=2.7",

    package_dir          = {"": "src"},
    packages             = [PACKAGE],
    include_package_data = True,  # Use MANIFEST.in

    classifiers          = [
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],

    long_description_content_type = "text/markdown",
    long_description              = get_description(),
)
