package cc.qxt063.blockStorageSimulator.utils;

import java.util.Random;

public class RandomUtils {
    private final static String charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private final static Random random = new Random();


    public static Integer randomInt(int maxExclusive) {
        return random.nextInt(maxExclusive);
    }

    public static Integer randomInt(int min, int maxExclusive) {
        return random.nextInt(maxExclusive - min) + min;
    }

    public static String randomString(Integer len) {
        //可以出现在字符串中的字符
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            int index = random.nextInt(charSet.length());
            char c = charSet.charAt(index);
            sb.append(c);
        }
        return sb.toString();
    }
}
