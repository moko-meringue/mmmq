package org.mmmq.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

// @formatter:off
/**
 * <P>이 클래스는 Spring Framework의 StringUtils 참고하여 수정/복사하였습니다.</P>
 * <a href="https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/util/StringUtils.html">원본 출처</a>
 */
// @formatter:on
public abstract class StringUtils {

    private static final String[] EMPTY_STRING_ARRAY = {};

    public static boolean isEmpty(Object str) {
        return (str == null || "".equals(str));
    }

    public static boolean hasLength(String str) {
        return (str != null && !str.isEmpty());
    }

    public static boolean hasText(String str) {
        return (str != null && !str.isBlank());
    }

    public static String replace(String inString, String oldPattern, String newPattern) {
        if (!hasLength(inString) || !hasLength(oldPattern) || newPattern == null) {
            return inString;
        }
        int index = inString.indexOf(oldPattern);
        if (index == -1) {
            return inString;
        }

        int capacity = inString.length();
        if (newPattern.length() > oldPattern.length()) {
            capacity += 16;
        }
        StringBuilder sb = new StringBuilder(capacity);

        int pos = 0;
        int patLen = oldPattern.length();
        while (index >= 0) {
            sb.append(inString, pos, index);
            sb.append(newPattern);
            pos = index + patLen;
            index = inString.indexOf(oldPattern, pos);
        }

        sb.append(inString, pos, inString.length());
        return sb.toString();
    }

    public static String[] toStringArray(Collection<String> collection) {
        return (!(collection == null || collection.isEmpty()) ? collection.toArray(EMPTY_STRING_ARRAY)
                : EMPTY_STRING_ARRAY);
    }

    public static String[] tokenizeToStringArray(
            String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {

        if (str == null) {
            return EMPTY_STRING_ARRAY;
        }

        StringTokenizer st = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || !token.isEmpty()) {
                tokens.add(token);
            }
        }
        return toStringArray(tokens);
    }
}
