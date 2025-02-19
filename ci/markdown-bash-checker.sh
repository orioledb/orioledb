#!/usr/bin/env bash
set -euo pipefail

# Show help message
function show_help() {
    cat << EOF
Usage: $(basename "$0") [options] <markdown-file>
Extract and optionally run bash code blocks from a markdown file.

Options:
  -h, --help          Show this help message
  -o, --output FILE   Specify the output file (default: auto-generated temporary file)
  -r, --run          Run the extracted bash code blocks
  --dry-run          Extract and show the blocks without executing them
  --check-syntax     Only check the bash syntax without running

Example:
  $(basename "$0") myfile.md             # Extract only
  $(basename "$0") -r myfile.md          # Extract and run
  $(basename "$0") --check-syntax myfile.md  # Check syntax only
EOF
}

# Function to check bash syntax
check_syntax() {
    local file="$1"
    if ! bash -n "$file"; then
        echo "Error: Syntax check failed for '$file'" >&2
        return 1
    fi
    echo "Syntax check passed for '$file'"
    return 0
}

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    show_help
    exit 1
fi

MARKDOWN_FILE=""
OUTPUT_FILE=""
RUN_BLOCKS=0
DRY_RUN=0
CHECK_SYNTAX=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        -o|--output)
            shift
            if [[ $# -eq 0 ]]; then
                echo "Error: Missing argument for output file" >&2
                exit 1
            fi
            OUTPUT_FILE="$1"
            shift
            ;;
        -r|--run)
            RUN_BLOCKS=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --check-syntax)
            CHECK_SYNTAX=1
            shift
            ;;
        *)
            MARKDOWN_FILE="$1"
            shift
            ;;
    esac
done

if [[ -z "$MARKDOWN_FILE" ]]; then
    echo "Error: Markdown file not specified." >&2
    show_help
    exit 1
fi

if [[ ! -f "$MARKDOWN_FILE" ]]; then
    echo "Error: File '$MARKDOWN_FILE' not found." >&2
    exit 1
fi

# If no output file is specified, use a temporary file
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE=$(mktemp --tmpdir _tempXXXX.sh)
fi

# Initialize the output file with the shebang and strict mode
{
    echo "#!/usr/bin/env bash"
    echo "set -euxo pipefail"
} > "$OUTPUT_FILE"

BLOCK_COUNT=0
in_bash_block=0

# Process the markdown file line by line
while IFS= read -r line; do
    # Strip leading/trailing whitespace
    trimmed_line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    
    # Detect start of a bash code block
    if [ "$trimmed_line" = '```bash' ]; then
        in_bash_block=1
        BLOCK_COUNT=$((BLOCK_COUNT + 1))
        echo -e "\n# Block $BLOCK_COUNT" >> "$OUTPUT_FILE"
        continue
    fi
    
    # Detect end of code block
    if [ "$trimmed_line" = '```' ] && [ $in_bash_block -eq 1 ]; then
        in_bash_block=0
        continue
    fi
    
    # If inside a bash block, write the line to the output file
    if [ $in_bash_block -eq 1 ]; then
        echo "$line" >> "$OUTPUT_FILE"
    fi
done < "$MARKDOWN_FILE"

# Make the output file executable
chmod +x "$OUTPUT_FILE"

# Provide feedback on the result
if [[ $BLOCK_COUNT -eq 0 ]]; then
    echo "Warning: No bash code blocks found in '$MARKDOWN_FILE'."
    exit 0
fi

echo "Found $BLOCK_COUNT bash code block(s), output written to '$OUTPUT_FILE'."

# Handle different execution modes
if [[ $CHECK_SYNTAX -eq 1 ]]; then
    check_syntax "$OUTPUT_FILE"
    exit $?
fi

if [[ $DRY_RUN -eq 1 ]]; then
    echo "Dry run - contents of extracted script:"
    cat "$OUTPUT_FILE"
    exit 0
fi

if [[ $RUN_BLOCKS -eq 1 ]]; then
    echo "Executing extracted bash blocks..."
    if ! "$OUTPUT_FILE"; then
        echo "Error: Execution failed" >&2
        exit 1
    fi
    echo "Execution completed successfully"
fi