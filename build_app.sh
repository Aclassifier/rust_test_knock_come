#!/bin/zsh

SOURCE_FILE="src/rust_test_knock_come.rs"

# Check if source file exists
if [ ! -f "$SOURCE_FILE" ]; then
    echo "❌ Error: $SOURCE_FILE not found!"
    exit 1
fi

# 1. Extract the version string from the source file
VERSION=$(grep 'const VERSION:' "$SOURCE_FILE" | sed -E 's/.*"([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
    echo "❌ Error: Could not extract VERSION from source code."
    exit 1
fi

# 2. Extract the CURRENT_SEMANTICS value from the source file
SEMANTICS=$(grep 'const CURRENT_SEMANTICS:' "$SOURCE_FILE" | sed -E 's/.*TaskSemantics::([^; ]+).*/\1/')
if [ -z "$SEMANTICS" ]; then
    echo "❌ Error: Could not extract CURRENT_SEMANTICS from source code."
    exit 1
fi

# 3. Dynamic Name Extraction (Simulating the match statement in your Rust file)
if [[ "$SEMANTICS" == *"MasterForceSendSlaveSelect"* ]]; then
    APP_NAME="KnockComeForce"
elif [[ "$SEMANTICS" == *"MasterTrySendSlaveSelect"* ]]; then
    APP_NAME="KnockComeTry"
else
    APP_NAME="KnockComeNested"
fi

echo "🍏 Successfully Parsed from Source:"
echo "   -> Version:   $VERSION"
echo "   -> Semantics: $SEMANTICS"
echo "   -> App Name:  $APP_NAME"
echo ""
echo "📝 Injecting targets into Cargo.toml..."

# 4. Use a flexible regex substitution that catches placeholders even with inline comments
perl -pi -e "s/APP_NAME_PLACEHOLDER/$APP_NAME/g" Cargo.toml
perl -pi -e "s/\"0\.1\.0\"/\"$VERSION\"/g" Cargo.toml

echo "🚀 Bundling macOS app..."
if cargo bundle --release; then
    echo "✨ Done! Opening target directory..."
    
    # 5. Restore default version and placeholders safely (retains comments)
    perl -pi -e "s/$APP_NAME/APP_NAME_PLACEHOLDER/g" Cargo.toml
    perl -pi -e "s/\"$VERSION\"/\"0.1.0\"/g" Cargo.toml
    
    # Open the compiled app location
    open target/release/bundle/osx/
else
    echo "❌ Error: 'cargo bundle' failed."
    
    # Fallback cleanup to prevent locking Cargo.toml into a bad state
    perl -pi -e "s/$APP_NAME/APP_NAME_PLACEHOLDER/g" Cargo.toml
    perl -pi -e "s/\"$VERSION\"/\"0.1.0\"/g" Cargo.toml
    exit 1
fi
