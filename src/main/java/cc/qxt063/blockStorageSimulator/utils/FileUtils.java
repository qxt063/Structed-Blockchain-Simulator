package cc.qxt063.blockStorageSimulator.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FileUtils {

    /**
     * 新建文件夹
     *
     * @param directoryPath
     * @return
     */
    public static boolean createFolder(String folderPath) {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            return folder.mkdirs();
        }
        return true;
    }

    /**
     * 文件数量
     *
     * @param folderPath
     * @return
     */
    public static int getFileCount(String folderPath) {
        File folder = new File(folderPath);
        if (folder.exists() && folder.isDirectory()) {
            return Objects.requireNonNull(folder.listFiles()).length;
        }
        return 0;
    }

    /**
     * 子文件夹数量
     *
     * @param folderPath
     * @return
     */
    public static int getFolderCount(String folderPath) {
        File folder = new File(folderPath);
        if (folder.exists() && folder.isDirectory()) {
            int count = 0;
            for (File file : Objects.requireNonNull(folder.listFiles())) {
                if (file.isDirectory()) {
                    count++;
                }
            }
            return count;
        }
        return 0;
    }

    /**
     * 删除文件
     *
     * @param filePath
     * @return
     */
    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        return file.delete();
    }

    /**
     * 新建文件
     *
     * @param filePath
     * @return
     */
    public static boolean createFile(String filePath) {
        File file = new File(filePath);
        try {
            return file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 写入文件
     *
     * @param filePath
     * @param data
     * @return
     */
    public static boolean writeToFile(String filePath, String data) {
        try {
            FileWriter writer = new FileWriter(filePath);
            writer.write(data);
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 读取文件
     *
     * @param filePath
     * @return
     */
    public static String readFromFile(String filePath) {
        try {
            FileReader reader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(reader);
            StringBuilder stringBuilder = new StringBuilder();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(System.getProperty("line.separator"));
            }
            bufferedReader.close();
            reader.close();
            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 复制文件
     *
     * @param sourcePath
     * @param destPath
     * @throws IOException
     */
    public static void copyFile(String sourcePath, String destPath) {
        File source = new File(sourcePath);
        File dest = new File(destPath);

        createFolder(dest.getParent());
        try {
            FileInputStream fis = new FileInputStream(source);
            FileOutputStream fos = new FileOutputStream(dest);

            // 缓冲区大小
            byte[] buffer = new byte[1024];
            int length;
            // 复制文件
            while ((length = fis.read(buffer)) > 0) {
                fos.write(buffer, 0, length);
            }
            fis.close();
            fos.close();

        } catch (IOException e) {
            System.err.println("文件复制失败：src=" + sourcePath + " dest=" + destPath);
        }
    }


    /**
     * 复制文件夹
     *
     * @param sourcePath
     * @param destPath
     */
    public static void copyFolder(String sourcePath, String destPath, boolean copySubFolder) {
        File source = new File(sourcePath);
        File dest = new File(destPath);

        // 创建目标文件夹
        createFolder(destPath);

        // 复制文件夹中的所有文件
        for (File file : Objects.requireNonNull(source.listFiles())) {
            if (copySubFolder && file.isDirectory()) {
                // 递归复制子文件夹
                copyFolder(file.getAbsolutePath(), destPath + File.separator + file.getName(), true);
            } else {
                // 复制文件
                copyFile(file.getAbsolutePath(), destPath + File.separator + file.getName());
            }
        }
    }

    public static List<String> getAllFiles(String folderPath) {
        File source = new File(folderPath);
        List<String> res = new ArrayList<>();
        for (File file : Objects.requireNonNull(source.listFiles())) {
            if (!file.isDirectory()) {
                res.add(file.getAbsolutePath());
            }
        }
        return res;
    }

}
