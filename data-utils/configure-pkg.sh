set -e

echo 'Bundling bindings in index.d.ts'

# Directory to loop through
BINDINGS_DIR="./bindings"     # Change this to your directory
INDEX_D_TS="./pkg/index.d.ts" # Change this to your target file

# Temporary file to hold intermediate results
temp_index_d_ts="$(mktemp)"
temp_extracted_part="$(mktemp)"

# Loop through each regular file in the directory
for file in "$BINDINGS_DIR"/*; do
  if [ -f "$file" ]; then
    # Extract everything after the first empty line
    awk 'found { print } NF==0 { found=1 }' "$file" >"$temp_extracted_part"

    # Only prepend if there's something to add
    if [ -s "$temp_extracted_part" ]; then
      {
        cat "$temp_extracted_part"
        cat "$INDEX_D_TS"
      } >"$temp_index_d_ts"

      mv "$temp_index_d_ts" "$INDEX_D_TS"
    fi

    # Remove file to make this idempotent
    rm "$file"
  fi
done

# Cleanup temp files and empty directories
rm -f "$temp_index_d_ts" "$temp_extracted_part"
[ -d "$BINDINGS_DIR" ] && rmdir "$BINDINGS_DIR"

echo "Making undefined arguments optional in index.d.ts"
sed -i -e 's/: undefined |/?:/g' pkg/index.d.ts

repository=${1:-$(git remote get-url origin)}
echo "Adding repository \"$repository\" to package.json"

tmp=$(mktemp)
jq ".repository = { \"type\": \"git\", \"url\": \"$repository\" }" pkg/package.json >"$tmp"
mv "$tmp" pkg/package.json
