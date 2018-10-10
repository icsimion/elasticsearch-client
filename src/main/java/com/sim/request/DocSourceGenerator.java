package com.sim.request;

import org.elasticsearch.common.collect.Tuple;

/**
 * Created by iliesimion
 */
public class DocSourceGenerator {
    public static final String CSV_SPLIT_CHAR = ",";
    public static final Character CSV_QUOTE_CHAR = '\"';
    public static final String CSV_ARRAY_SEP_CHAR = "\\|";

    public static String[] splitArrayField(final String value) {
        return value.split(CSV_ARRAY_SEP_CHAR);
    }

    public static Tuple<String, Integer> getTitleYear(final String value) {
        //title & year
        String title = value.substring(value.indexOf(CSV_SPLIT_CHAR) + 1, value.lastIndexOf(CSV_SPLIT_CHAR));
        if (title.charAt(0) == CSV_QUOTE_CHAR && title.charAt(title.length() - 1) == CSV_QUOTE_CHAR) {
            title = title.substring(1, title.length() - 1);
        }

        int indexOfParanthesisOpen = title.indexOf("(");
        int indexOfParanthesisClosed = title.indexOf(")");
        String year = "1900";

        if (indexOfParanthesisOpen > -1 && indexOfParanthesisClosed > indexOfParanthesisOpen) {
            year = title.substring(indexOfParanthesisOpen + 1, indexOfParanthesisClosed);
            title = title.substring(0, indexOfParanthesisOpen);
        }

        return new Tuple(title.trim(), year);
    }

}
