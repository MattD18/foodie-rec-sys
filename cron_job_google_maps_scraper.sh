#!/bin/bash
source /home/mattgeorgedalton/miniconda3/etc/profile.d/conda.sh
conda activate foodie
cd /home/mattgeorgedalton/foodie-rec-sys/scrapers
python google_maps_scraper.py
