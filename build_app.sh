#!/bin/zsh

SOURCE_FILE="src/rust_test_knock_come.rs"

# Check if source file exists
if [ ! -f "$SOURCE_FILE" ]; then
    echo "❌ Error: $SOURCE_FILE not found!"
    exit 1
fi

# Crash recovery: If a backup file exists from a previous crash, restore it first
if [ -f "Cargo.toml.bak" ]; then
    echo "⚠️ Warning: Found Cargo.toml.bak from a previous crash! Restoring original Cargo.toml..."
    mv Cargo.toml.bak Cargo.toml
fi

# 1. Extract the version string from the source file
VERSION=$(grep 'const VERSION:' "$SOURCE_FILE" | sed -E 's/.*"([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
    echo "❌ Error: Could not extract VERSION from source code."
    exit 1
fi

# 2. Extract the current semantics enum variant
SEMANTICS=$(grep 'const CURRENT_SEMANTICS:' "$SOURCE_FILE" | sed -E 's/.*TaskSemantics::([^; ]+).*/\1/')
if [ -z "$SEMANTICS" ]; then
    echo "❌ Error: Could not extract CURRENT_SEMANTICS from source code."
    exit 1
fi

# 3. Extract APP_NAME dynamically from the CURRENT_APP_NAME match block
# This looks strictly inside the definition block of CURRENT_APP_NAME in your Rust code
# The sed scan causes a file-access event; even though it is read-only, it makes the file visibly "flip" or blink in VS Code
APP_NAME=$(sed -n '/const CURRENT_APP_NAME/,/};/p' "$SOURCE_FILE" | grep "TaskSemantics::$SEMANTICS" | sed -E 's/.*=> *"([^"]+)".*/\1/')

# Strict error check: If extraction fails, stop immediately instead of using hardcoded defaults
if [ -z "$APP_NAME" ]; then
    echo "❌ Error: Could not dynamically extract APP_NAME from CURRENT_APP_NAME match block!"
    echo "   Please check that the formatting in $SOURCE_FILE matches the pattern."
    exit 1
fi

# Format the process binary name to lowercase for Activity Monitor
BIN_NAME=$(echo "$APP_NAME" | tr '[:upper:]' '[:lower:]')

echo "🍏 Found in source code:"
echo "   -> Version:     $VERSION"
echo "   -> Semantics:   $SEMANTICS"
echo "   -> App Bundle:  $APP_NAME.app"
echo "   -> Process ID:  $BIN_NAME"
echo ""

echo "📝 Creating backup and temporarily injecting targets into Cargo.toml..."
# Create a secure backup of the untouched Cargo.toml
cp Cargo.toml Cargo.toml.bak

# 4. Inject variables into Cargo.toml placeholders safely
perl -pi -e "s/\"APP_NAME_PLACEHOLDER\"/\"$APP_NAME\"/g" Cargo.toml
perl -pi -e "s/\"BIN_NAME_PLACEHOLDER\"/\"$BIN_NAME\"/g" Cargo.toml
perl -pi -e "s/\"0\.1\.0\"/\"$VERSION\"/g" Cargo.toml

echo "🚀 Bundling macOS app..."
cargo bundle --release
BUILD_STATUS=$?

# 5. ALWAYS restore the original Cargo.toml from backup, regardless of success or failure
mv Cargo.toml.bak Cargo.toml

if [ $BUILD_STATUS -eq 0 ]; then
    echo "✨ Done! Opening target directory..."
    # Open the compiled app location
    open target/release/bundle/osx/
else
    echo "❌ Error: 'cargo bundle' failed."
    exit 1
fi
