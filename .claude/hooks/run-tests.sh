#!/bin/bash

# Java 파일이 수정된 경우에만 테스트를 실행한다
if ! git status --short 2>/dev/null | grep -q '\.java'; then
    exit 0
fi

OUTPUT=$(./gradlew test -q 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "테스트가 실패했습니다. 원인을 파악하고 수정해주세요."
    echo ""
    echo "$OUTPUT"
    exit 1
fi

exit 0