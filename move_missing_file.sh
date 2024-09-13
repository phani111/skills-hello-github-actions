diff -rq ~/Documents/folder1 ~/Documents/folder2 | grep "Only in ~/Documents/folder1" | awk '{print $3 "/" $4}' | while read filepath; do cp "$filepath" ~/Documents/folder2/; done
