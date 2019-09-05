
package com.elefana.util;

import org.junit.Test;
import org.junit.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoAllocStringReplaceTest {

    final String testString = "A test String";

    @Test
    public void testStringContainingSearchString() {
        final String[] search = {"test"};
        assertStringContainsSearch(testString, search);
    }

    @Test
    public void testStringNotContainingSearchString() {
        final String[] search = {"Not in here"};
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testStringContainingBothSearchStrings() {
        final String[] search = { "A", "test" };
        assertStringContainsSearch(testString, search);
    }

    @Test
    public void testStringContainingOnlyOneSearchString() {
        final String[] search = { "test", "Not in here" };
        assertStringContainsSearch(testString, search);
    }

    @Test
    public void testStringNotContainingBothSearchStrings() {
        final String[] search = { "Not in here", "Also not there" };
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testSearchStringArrayIsEmpty() {
        final String[] search = {};
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testSearchStringIsEmpty() {
        final String[] search = { "" };
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testStringIsEmpty() {
        final String emptyTestString = "";
        final String[] search = { "test" };
        assertStringDoesNotContainSearch(emptyTestString, search);
    }

    @Test
    public void testStringIsNull() {
        final String nullTestString = null;
        final String[] search = { "test" };
        assertStringDoesNotContainSearch(nullTestString, search);
    }

    @Test
    public void testSearchStringArrayIsNull() {
        final String[] search = null;
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testSearchStringIsNull() {
        final String[] search = { null };
        assertStringDoesNotContainSearch(testString, search);
    }

    @Test
    public void testSearchStringIsIgnoredIfNull() {
        final String[] search = { null, "test" };
        assertStringContainsSearch(testString, search);
    }

    private void assertStringContainsSearch(String string, String [] search) {
        Assert.assertTrue(NoAllocStringReplace.contains(string, search));
    }

    private void assertStringDoesNotContainSearch(String string, String [] search) {
        Assert.assertFalse(NoAllocStringReplace.contains(string, search));
    }

    @Test
    public void testReplace() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a l\nong string");
        str.replaceAndEscapeUnicode(new String[] { "\n" }, new String[] { "\\\\n" });
        String ret = str.dispose();

        Assert.assertEquals("this is a l\\\\nong string", ret);
    }

    @Test
    public void testReplaceInTheEnd() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string foo");
        str.replaceAndEscapeUnicode(new String[] { "foo" }, new String[] { "hello" });
        String ret = str.dispose();

        Assert.assertEquals("this is a long string hello", ret);
    }

    @Test
    public void testReplaceSmallerLengthInTheEnd() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] { "a long string" }, new String[] { "funny" });
        String ret = str.dispose();

        Assert.assertEquals("this is funny", ret);
    }

    @Test
    public void testReplaceSmallerLength() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string blah");
        str.replaceAndEscapeUnicode(new String[] { "a long string" }, new String[] { "funny" });
        String ret = str.dispose();

        Assert.assertEquals("this is funny blah", ret);
    }

    @Test
    public void testReplaceMultiple() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a l\nong str\ning");
        str.replaceAndEscapeUnicode(new String[] { "\n" }, new String[] { "\\\\n" });
        String ret = str.dispose();

        Assert.assertEquals("this is a l\\\\nong str\\\\ning", ret);
    }

    @Test
    public void testReplaceWithMultipleSearchStrings() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a l\nong str\ring");
        str.replaceAndEscapeUnicode(new String[] { "\n", "\r" }, new String[] { "\\\\n", "\\\\r" });
        String ret = str.dispose();

        Assert.assertEquals("this is a l\\\\nong str\\\\ring", ret);
    }

    @Test
    public void testReplaceMultipleWithMultipleSearchStrings() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("thi\ns is\r a l\nong str\ring");
        str.replaceAndEscapeUnicode(new String[] { "\n", "\r" }, new String[] { "\\\\n", "\\\\r" });
        String ret = str.dispose();

        Assert.assertEquals("thi\\\\ns is\\\\r a l\\\\nong str\\\\ring", ret);
    }

    @Test
    public void testEscapeUnicodeCharacter() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("abd\\u8932eeee");
        str.replaceAndEscapeUnicode(new String[] { "b" }, new String[] { "xNewx" });
        String ret = str.dispose();

        Assert.assertEquals("axNewxd\\\\u8932eeee", ret);
    }

    @Test
    public void testEmptyString() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("");
        str.replaceAndEscapeUnicode(new String[] { "b" }, new String[] { "xNewx" });
        String ret = str.dispose();

        Assert.assertEquals("", ret);
    }

    @Test
    public void testEmptyArrays() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] {  }, new String[] {  });
        String ret = str.dispose();

        Assert.assertEquals("this is a long string", ret);
    }

    @Test
    public void testEmptyStringInSearchArray() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] { "" }, new String[] { "abc" });
        String ret = str.dispose();

        Assert.assertEquals("this is a long string", ret);
    }

    @Test
    public void testNullInSearchArray() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] { null }, new String[] { "abc" });
        String ret = str.dispose();

        Assert.assertEquals("this is a long string", ret);
    }

    @Test
    public void testNullInReplaceArray() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] { "a long" }, new String[] { null });
        String ret = str.dispose();

        Assert.assertEquals("this is a long string", ret);
    }

    @Test
    public void testDelete() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate("this is a long string");
        str.replaceAndEscapeUnicode(new String[] { "a long" }, new String[] { "" });
        String ret = str.dispose();

        Assert.assertEquals("this is  string", ret);
    }

    @Test(expected = NullPointerException.class)
    public void testNullString() {
        NoAllocStringReplace str = NoAllocStringReplace.allocate(null);
        str.replaceAndEscapeUnicode(new String[] { "b" }, new String[] { "xNewx" });
        String ret = str.dispose();
    }
}
