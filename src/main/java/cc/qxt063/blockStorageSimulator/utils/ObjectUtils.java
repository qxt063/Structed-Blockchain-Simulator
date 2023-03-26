package cc.qxt063.blockStorageSimulator.utils;

import java.lang.reflect.Field;
import java.util.TreeMap;

public class ObjectUtils {
    public static <T> TreeMap<String, Object> toMap(T object) throws IllegalAccessException {
        TreeMap<String, Object> map = new TreeMap<>();
        Class<?> clazz = object.getClass();

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String name = field.getName();
            Object value = field.get(object);
            map.put(name, value);
        }
        return map;
    }

    public static <T> TreeMap<String, String> toStringMap(T object) throws IllegalAccessException {
        TreeMap<String, String> map = new TreeMap<>();
        Class<?> clazz = object.getClass();

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String name = field.getName();
            String value = field.get(object).toString();
            map.put(name, value);
        }
        return map;
    }
}