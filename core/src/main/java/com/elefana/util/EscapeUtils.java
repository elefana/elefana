/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.util;

import com.fasterxml.jackson.core.io.CharacterEscapes;

public class EscapeUtils {
	private final static int[] ASCII_ESCAPE_CHAR_POINT_CODES;

	private final static char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

	public static final String [] ESCAPE_SEARCH = new String [] {
			"\\\"",
			"\n \"",
			"\n  \"",
			"\n   \"",
			"\n    \"",
			"\n",
			"\r",
			"\t",
			"\f",
			"\b",
			"\u0000"
	};
	public static final String [] ESCAPE_REPLACE = new String [] {
			"\\\\\\\"",
			" \"",
			" \"",
			" \"",
			" \"",
			"\\\\n",
			"\\\\r",
			"\\\\t",
			"\\\\f",
			"\\\\b",
			""
	};
	public static final String [] ESCAPE_PRE_ESCAPE = new String [] {
			"\\\\\\\"",
			"\\\\n",
			"\\\\r",
			"\\\\t",
			"\\\\f",
			"\\\\b"
	};

	static {
		int[] table = new int[128];
		for (int i = 0; i < 32; ++i) {
			table[i] = CharacterEscapes.ESCAPE_STANDARD;
		}
		table['"'] = '"';
		table['\\'] = '\\';
		table[0x08] = 'b';
		table[0x09] = 't';
		table[0x0C] = 'f';
		table[0x0A] = 'n';
		table[0x0D] = 'r';
		ASCII_ESCAPE_CHAR_POINT_CODES = table;
	}

	public static void appendQuoted(StringBuilder sb, char [] content, int contentOffset, int contentLength)
	{
		final int[] escCodes = ASCII_ESCAPE_CHAR_POINT_CODES;
		int escLen = escCodes.length;
		for (int i = contentOffset, len = contentOffset + contentLength; i < len; ++i) {
			char c = content[i];
			if (c >= escLen || escCodes[c] == 0) {
				sb.append(c);
				continue;
			}
			sb.append('\\');
			int escCode = escCodes[c];
			if (escCode < 0) {
				sb.append('u');
				sb.append('0');
				sb.append('0');
				int value = c;
				sb.append(HEX_CHARS[value >> 4]);
				sb.append(HEX_CHARS[value & 0xF]);
			} else {
				sb.append((char) escCode);
			}
		}
	}

	public static String psqlUnescapeString(String json) {
		for(int i = 0; i < json.length(); i++) {
			if(json.charAt(i) != '\\') {
				continue;
			}
			switch(json.charAt(i + 1)) {
			case '\\':
				if(i + 2 >= json.length()) {
					continue;
				}
				switch(json.charAt(i + 2)) {
				case '\\':
					if(i + 3 >= json.length()) {
						continue;
					}
					switch(json.charAt(i + 3)) {
					case '\"':
						json = json.substring(0, i) + json.substring(i + 2);
						continue;
					}
					break;
				case 0x85:
				case 'n':
					if(i > 0) {
						json = json.substring(0, i) + '\n' + json.substring(i + 3);
					} else {
						json = '\n' + json.substring(i + 3);
					}
					break;
				case 'r':
					if(i > 0) {
						json = json.substring(0, i) + '\r' + json.substring(i + 3);
					} else {
						json = '\r' + json.substring(i + 3);
					}
					break;
				case 't':
					if(i > 0) {
						json = json.substring(0, i) + '\t' + json.substring(i + 3);
					} else {
						json = '\t' + json.substring(i + 3);
					}
					break;
				case 'f':
					if(i > 0) {
						json = json.substring(0, i) + '\f' + json.substring(i + 3);
					} else {
						json = '\f' + json.substring(i + 3);
					}
					break;
				case 'b':
					if(i > 0) {
						json = json.substring(0, i) + '\b' + json.substring(i + 3);
					} else {
						json = '\b' + json.substring(i + 3);
					}
					break;
				case 'u':
					if(i > 0) {
						json = json.substring(0, i) + json.substring(i + 1);
					} else {
						json = json.substring(i + 1);
					}
					break;
				}
				break;
			default:
				continue;
			}
		}
		return json;
	}

	/**
	 * If JSON string contains \" we need to escape it as \\" for PSQL to handle correctly
	 * @param json The original JSON string
	 * @return The escaped JSON string
	 */
	public static String psqlEscapeString(String json) {
		if(NoAllocStringReplace.contains(json, ESCAPE_PRE_ESCAPE)) {
			return json;
		}
		final NoAllocStringReplace str = NoAllocStringReplace.allocate(json);
		str.replaceAndEscapeUnicode(ESCAPE_SEARCH, ESCAPE_REPLACE);
		return str.disposeWithResult();
	}
}
