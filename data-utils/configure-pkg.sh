set -e

echo 'Add "binding.ts" to package.json files'

repository=${1:-$(git remote get-url origin)}
echo "Adding repository \"$repository\" to package.json"

org=$2
if [ -n "$org" ]; then
  echo "Setting package scope to \"@$org\""
  org="@$org/"
fi

tmp=$(mktemp)
jq \
  ".files |= (.+ [\"bindings.ts\"] | unique) |
  .repository = { \"type\": \"git\", \"url\": \"$repository\" } |
  .name |= \"$org\" + (. | split(\"/\"))[-1]" \
  pkg/package.json >"$tmp"
mv "$tmp" pkg/package.json

echo "Making undefined arguments optional in index.d.ts"
sed -i -e 's/: undefined |/?:/g' pkg/index.d.ts
