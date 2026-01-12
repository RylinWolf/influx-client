package com.wolfhouse.influxclient.comparator;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串类型的自然排序器
 * 在自然排序的基础上，对于数字后缀内容按照数字自然排序
 *
 * @author Rylin Wolf
 */
public class NaturalComparator implements Comparator<String> {
    Pattern p = Pattern.compile("\\d+");

    @Override
    public int compare(String a, String b) {
        // 提取数字部分的简单正则表达式
        Matcher ma = p.matcher(a);
        Matcher mb = p.matcher(b);

        if (ma.find() && mb.find()) {
            // 先比较非数字前缀
            String prefixA = a.substring(0, ma.start());
            String prefixB = b.substring(0, mb.start());
            if (!prefixA.equals(prefixB)) {
                return prefixA.compareTo(prefixB);
            }

            // 前缀相同时，比较数字数值
            return Integer.compare(
                    Integer.parseInt(ma.group()),
                    Integer.parseInt(mb.group())
            );
        }
        return a.compareTo(b);
    }
}
