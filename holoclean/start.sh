#!/usr/bin/env bash
# Set & move to home directory
source set_env.sh

# First, run the ehr_extracttion.py script
echo "Running ehr_extract.py"
python examples/ehr_extract.py

# After that, run the main script (default or provided as argument)
script="examples/holoclean_repair_example.py"
if [ $# -eq 1 ] ; then
  script="$1"
fi

echo "Launching example script $script"
python $script
