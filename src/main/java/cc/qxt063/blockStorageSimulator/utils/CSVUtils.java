package cc.qxt063.blockStorageSimulator.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CSVUtils {
    /**
     * 合并打印list到csv
     *
     * @param filePath
     * @param header
     * @param lists
     */
    public static void lists2Csv(String filePath, List<String> header, List<List<Long>> lists) {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.append(String.join(",", header));
            writer.append("\n");
            for (int i = 0; i < lists.get(0).size(); i++) {
                for (int j = 0; j < lists.size(); j++) {
                    List<Long> row = lists.get(j);
                    writer.append(row.get(i).toString());
                    if (j < lists.size() - 1) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
