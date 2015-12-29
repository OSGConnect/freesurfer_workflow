#!/bin/bash

cp -a ../bash .
python setup.py bdist_rpm
rm -fr bash
