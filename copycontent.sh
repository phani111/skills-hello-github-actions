#!/bin/bash

# The source properties file
source_file="your_source_file.properties"

# Check if the source file exists
if [ ! -f "$source_file" ]; then
    echo "Source file $source_file does not exist."
    exit 1
fi

# Loop through all .properties files in the current directory
for file in *.properties
do
    # Skip the source file
    if [ "$file" != "$source_file" ]; then
        echo "Processing $file"
        
        # Copy the entire content of the source file to the current file
        cp "$source_file" "$file"
        
        # Get the filename without extension
        filename_without_ext="${file%.properties}"
        
        # Update the specific lines
        sed -i "s/^mappingfilename=.*/mappingfilename=${filename_without_ext}.csv/" "$file"
        sed -i "s/^filename=.*/filename=${file}/" "$file"
        
        echo "Updated $file"
    fi
done

echo "Done processing all .properties files."
