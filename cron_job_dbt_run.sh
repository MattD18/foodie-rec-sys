#!/bin/bash
source /home/mattgeorgedalton/miniconda3/etc/profile.d/conda.sh
conda activate foodie
cd /home/mattgeorgedalton/foodie-rec-sys/data_warehouse
dbt run
