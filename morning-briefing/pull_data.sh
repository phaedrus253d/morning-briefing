#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate science
python main.py --update
