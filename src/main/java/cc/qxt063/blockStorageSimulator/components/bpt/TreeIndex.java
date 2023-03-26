package cc.qxt063.blockStorageSimulator.components.bpt;

import cc.qxt063.blockStorageSimulator.components.bpt.bptree.*;
import cc.qxt063.blockStorageSimulator.components.bpt.util.InvalidBTreeStateException;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * B+树索引
 */
public class TreeIndex {
    private BPlusConfiguration bConf;
    private BPlusTreePerformanceCounter bPerf;
    private BPlusTree tree;
    private String filePath;
    /**
     * 唯一约束
     */
    private boolean unique;

//    public TreeIndex(String filePath, boolean unique) {
//        this(filePath, unique, false);
//    }

    public TreeIndex(String filePath, boolean unique, boolean recreateTree) {
        this(filePath, unique, 1024, recreateTree);
    }

    public TreeIndex(String filePath, boolean unique, Integer pageSize, boolean recreateTree) {
        this.filePath = filePath;
        this.unique = unique;
        bConf = new BPlusConfiguration(pageSize);
        bPerf = new BPlusTreePerformanceCounter(false);
        initTree(recreateTree);
    }

    private void initTree(boolean recreateTree) {
        try {
            tree = new BPlusTree(bConf, recreateTree ? "rw+" : "rw", this.filePath, bPerf);
        } catch (IOException | InvalidBTreeStateException e) {
            System.err.println("error to build or open an index tree");
        }

    }

    public boolean insert(Long key, String value) {
        try {
            tree.insertKey(key, value, this.unique);
        } catch (IOException | InvalidBTreeStateException e) {
            System.err.println("insert error");
            return false;
        }
        return true;
    }

    public List<String> findByKey(Long key) {
        SearchResult sr;
        try {
            sr = tree.searchKey(key, this.unique);
        } catch (IOException | InvalidBTreeStateException e) {
            System.err.println("search error");
            return null;
        }
        if (sr.getValues() == null) {
            return new ArrayList<>();
        }
        List<String> res = new ArrayList<>(sr.getValues().size());
        for (String v : sr.getValues()) {
            res.add(v.trim());
        }
        return res;
    }

    // Map<key,List<value>>
    public TreeMap<Long, List<String>> findByRange(Long min, Long maxInclude) {
        RangeResult rr;
        try {
            rr = tree.rangeSearch(min, maxInclude, this.unique);
        } catch (IOException | InvalidBTreeStateException e) {
            System.err.println("search error");
            return null;
        }
//        rr.getQueryResult().forEach(x -> System.out.printf("kv(%d,%s), ", x.getKey(), x.getValue().trim()));
        TreeMap<Long, List<String>> res = new TreeMap<>();
        for (KVPair entry : rr.getQueryResult()) {
            if (!res.containsKey(entry.getK()))
                res.put(entry.getK(), new ArrayList<>());
            res.get(entry.getK()).add(entry.getV().trim());
        }
        return res;
    }


    public Iterator<List<KVPair>> iterator() {
        return tree.iterator();
    }

    //  List<ptr>
    public Map<Long, List<String>> getAllNodes() {
        return tree.getAllNodes();
    }

    // List<Height:ptr>
    public Map<Long, List<String>> getAllNodesWithHeight(int height) {
        Map<Long, List<String>> res = new HashMap<>();
        for (List<KVPair> kvp : tree) {
            res.put(kvp.get(0).getK(), kvp.stream().map(kvPair -> height + ":" + kvPair.getV()).collect(Collectors.toList()));
        }
        return res;
    }


    public String getIndexPath() {
        return this.filePath;
    }

    public Long getPageSize() {
        return tree.getTotalTreePages();
    }

    public void backupIndex(String backupFilePath) {
        if (!close()) return;
        FileUtils.copyFile(this.filePath, backupFilePath);
        initTree(false);
    }

    public boolean close() {
        try {
            tree.commitTree();
            tree = null;
        } catch (IOException | InvalidBTreeStateException e) {
            System.err.println("commit error");
            return false;
        }
        return true;
    }
}
