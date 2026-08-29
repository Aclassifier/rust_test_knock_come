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

# 2. Extract the current semantics enum variant
SEMANTICS=$(grep 'const CURRENT_SEMANTICS:' "$SOURCE_FILE" | sed -E 's/.*TaskSemantics::([^; ]+).*/\1/')
if [ -z "$SEMANTICS" ]; then
    echo "❌ Error: Could not extract CURRENT_SEMANTICS from source code."
    exit 1
fi

# 3. Determine the application name based on the extracted semantics value
if [[ "$SEMANTICS" == *"MasterForceSendSlaveSelect"* ]]; then
    APP_NAME="KnockComeForce"
elif [[ "$SEMANTICS" == *"MasterTrySendSlaveSelect"* ]]; then
    APP_NAME="KnockComeTry"
else
    APP_NAME="KnockComeNested"
fi

echo "🍏 Found in source code:"
echo "   -> Version:   $VERSION"
echo "   -> Semantics: $SEMANTICS"
echo "   -> App Name:  $APP_NAME"
echo ""
echo "📝 Temporarily injecting values into Cargo.toml..."

# 4. Inject the correct App Name and Version into Cargo.toml safely using awk
# This guarantees your comments and quotes stay perfectly intact!
awk -v ver="$VERSION" -v app="$APP_NAME" '{
    if ($1 == "version" && $2 == "=" && $3 == "\"0.1.0\"") {
        sub(/"0.1.0"/, "\"" ver "\"");
    }
    if ($1 == "name" && $2 == "=" && $3 == "\"APP_NAME_PLACEHOLDER\"") {
        sub(/"APP_NAME_PLACEHOLDER"/, "\"" app "\"");
    }
    print;
}' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml

echo "🚀 Bundling macOS app..."
if cargo bundle --release; then
    echo "✨ Done! Opening target directory..."
    
    # 5. Restore default version and placeholders safely
    awk -v ver="$VERSION" -v app="$APP_NAME" '{
        if ($1 == "version" && $2 == "=" && $3 == "\"" ver "\"") {
            sub("\"" ver "\"", "\"0.1.0\"");
        }
        if ($1 == "name" && $2 == "=" && $3 == "\"" app "\"") {
            sub("\"" app "\"", "\"APP_NAME_PLACEHOLDER\"");
        }
        print;
    }' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml
    
    # Open the compiled app location
    open target/release/bundle/osx/
else
    echo "❌ Error: 'cargo bundle' failed."
    
    # Restore even if compilation fails to keep Cargo.toml stable
    awk -v ver="$VERSION" -v app="$APP_NAME" '{
        if ($1 == "version" && $2 == "=" && $3 == "\"" ver "\"") {
            sub("\"" ver "\"", "\"0.1.0\"");
        }
        if ($1 == "name" && $2 == "=" && $3 == "\"" app "\"") {
            sub("\"" app "\"", "\"APP_NAME_PLACEHOLDER\"");
        }
        print;
    }' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml
    exit 1
fi
