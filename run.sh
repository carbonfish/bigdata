#/usr/bin/env bash

# Generate ~10GB data
time yarn jar build/libs/bigdata-1.0-all.jar gen 100000000 /data/gen_1G_12

# Sort the data
time yarn jar build/libs/bigdata-1.0-all.jar sort /data/gen_1G_12 /data/sort_1G_12

# Validate the data
time yarn jar build/libs/bigdata-1.0-all.jar validate /data/sort_1G_12 /data/valid_1G_12
