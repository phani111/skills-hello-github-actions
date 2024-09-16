#!/bin/bash

# The source properties file
source_file="c1_policy_benefit_config.properties"

# Check if the source file exists
if [ ! -f "$source_file" ]; then
    echo "Source file $source_file does not exist."
    exit 1
fi

# Loop through all .properties files in the current directory
for file in *.properties; do
    # Skip the source file
    if [ "$file" != "$source_file" ]; then
        echo "Processing $file"
        
        # Copy the entire content of the source file to the current file
        if ! cp "$source_file" "$file"; then
            echo "Failed to copy $source_file to $file"
            continue
        fi
        
        # Get the filename without extension
        filename_without_ext="${file%.properties}"
        filename_base="${filename_without_ext}_config"

        if ! awk -v csv="${filename_base}.csv" \
            -v prop="$file" \
            -v bqtable="${filename_base}" \
            -v folder="history/fed_macdonald_data/macdonald_consumpt/${filename_base}" \
            '
            /mappingFileName/ {print "mappingFileName=" csv; next}
            /bqtable/ {print "bqtable=" bqtable; next}
            /filename/ {print "filename=" prop; next}
            /folder/ {print "folder=" folder; next}
            {print}
            ' "$file" > "${file}.tmp"; then
            echo "Failed to process $file with AWK"
            continue
        fi

        if ! mv "${file}.tmp" "$file"; then
            echo "Failed to move temporary file to $file"
            continue
        fi
        
        echo "Updated $file"
    fi
done

echo "Done processing all .properties files."
