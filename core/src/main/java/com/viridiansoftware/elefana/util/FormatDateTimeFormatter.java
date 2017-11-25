/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.util;

import java.util.Locale;
import java.util.Objects;

import org.joda.time.format.DateTimeFormatter;

public class FormatDateTimeFormatter {
    private final String format;
    private final DateTimeFormatter parser;
    private final DateTimeFormatter printer;
    private final Locale locale;

    public FormatDateTimeFormatter(String format, DateTimeFormatter parser, Locale locale) {
        this(format, parser, parser, locale);
    }

    public FormatDateTimeFormatter(String format, DateTimeFormatter parser, DateTimeFormatter printer, Locale locale) {
        this.format = format;
        this.locale = Objects.requireNonNull(locale, "A locale is required as JODA otherwise uses the default locale");
        this.printer = printer.withLocale(locale).withDefaultYear(1970);
        this.parser = parser.withLocale(locale).withDefaultYear(1970);
    }

    public String format() {
        return format;
    }

    public DateTimeFormatter parser() {
        return parser;
    }

    public DateTimeFormatter printer() {
        return this.printer;
    }

    public Locale locale() {
        return locale;
    }
}
