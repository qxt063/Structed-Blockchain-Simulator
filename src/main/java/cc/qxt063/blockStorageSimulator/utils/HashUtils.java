package cc.qxt063.blockStorageSimulator.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {
    public static byte[] sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to calculate hash", e);
        }
    }

    // 计算SHA-256哈希值
    public static <T> byte[] sha256(T obj) {
        // 将对象转换为字节数组
        String s = obj.toString();
//        System.out.println(s);
        return sha256(s.getBytes(StandardCharsets.UTF_8));
    }


    public static <T> String sha256Hex(T obj) {
        byte[] hash = sha256(obj);
        return ByteUtils.bytesToHexString(hash);
    }
}
