package com.gs.photos.ws;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestHbaseStatsDAO {
    protected static String REG_EXP = "Y:([0-9]+)\\/M\\:([0-9]+)\\/D\\:([0-9]+)\\/H\\:([0-9]+)\\/Mn\\:([0-9]+)";

    @Test
    public void test001() {

        Pattern p = Pattern.compile(TestHbaseStatsDAO.REG_EXP);
        Matcher m = p.matcher("Y:2020/M:5/D:10/H:9/Mn:1");
        boolean b = m.matches();
        if (b) {
            for (int i = 0; i <= m.groupCount(); i++) {
                System.out.println("Groupe " + i + " : " + m.group(i));
            }
        }
    }

}
