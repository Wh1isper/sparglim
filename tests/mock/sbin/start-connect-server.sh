#!/usr/bin/env bash

script_name=$0
script_full_path=$(dirname "$0")

python $script_full_path/entrypoint.py
