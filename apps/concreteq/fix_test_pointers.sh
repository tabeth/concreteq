#!/bin/bash
FILES=$(find . -maxdepth 2 -name "*.go")

for f in $FILES; do
  if [ -f "$f" ]; then
    echo "Processing $f"
    # Numbers (including negative) with flexible spacing
    sed -E -i 's/MaxNumberOfMessages:[[:space:]]*(-?[0-9]+)/MaxNumberOfMessages: models.Ptr(\1)/g' "$f"
    sed -E -i 's/DelaySeconds:[[:space:]]*(-?[0-9]+)/DelaySeconds: models.Ptr(\1)/g' "$f"
    sed -E -i 's/VisibilityTimeout:[[:space:]]*(-?[0-9]+)/VisibilityTimeout: models.Ptr(\1)/g' "$f"
    sed -E -i 's/WaitTimeSeconds:[[:space:]]*(-?[0-9]+)/WaitTimeSeconds: models.Ptr(\1)/g' "$f"
    sed -E -i 's/MaxResults:[[:space:]]*(-?[0-9]+)/MaxResults: models.Ptr(\1)/g' "$f"
    
    # Strings
    sed -E -i 's/MessageGroupId:[[:space:]]*"([^"]*)"/MessageGroupId: models.Ptr("\1")/g' "$f"
    sed -E -i 's/MessageDeduplicationId:[[:space:]]*"([^"]*)"/MessageDeduplicationId: models.Ptr("\1")/g' "$f"
    
    # Int32 pointer handling
    sed -i 's/&delaySeconds/models.Ptr(int(*delaySeconds))/g' "$f"
    sed -i 's/&delay/models.Ptr(int(*delay))/g' "$f"

    # Specific test case fixes
    sed -E -i 's/VisibilityTimeout:[[:space:]]*tc.timeout/VisibilityTimeout: models.Ptr(tc.timeout)/g' "$f"
  fi
done
