package cc.qxt063.blockStorageSimulator.utils;

import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static cc.qxt063.blockStorageSimulator.utils.ByteUtils.bytesToHexString;
import static cc.qxt063.blockStorageSimulator.utils.ByteUtils.concatBytes;
import static cc.qxt063.blockStorageSimulator.utils.HashUtils.sha256;

public class MerkleUtils {

    private static byte[] hash1 = "d9f805d6cb07752f4e622fddfe50c79db35e59b3dadc0602dd91aa2c14882b91".getBytes(StandardCharsets.UTF_8),
            hash2 = "8e2a137bb41eb9eef2247890e91aa81590d99ce67aae733e4068288dae7b7237".getBytes(StandardCharsets.UTF_8);

//    public static String buildMerkle(List<Transaction> txs) {
//        if (txs.size() % 2 == 1) txs.add(new NullTransaction());
//        // 计算哈希值并构建Merkle树
//        byte[] merkleRoot = txs.stream()
//                .map(tx -> sha256(tx))
//                .reduce((left, right) -> sha256(concatBytes(left, right)))
//                .orElse(new byte[0]);
//        return bytesToHexString(merkleRoot);
//    }
//

    // 构建Merkle树
    public static byte[] buildMerkle(List<Transaction> txs) {
        // 如果只有一个字符串，直接返回其SHA-256哈希值
        if (txs.size() == 0) return new byte[0];
        if (txs.size() == 1) {
            return sha256(txs.get(0));
        }
//        if (txs.size() % 2 == 1) txs.add(new NullTransaction());

        // 计算左右子树的哈希值
        int mid = txs.size() / 2;
        List<Transaction> ltx = txs.subList(0, mid);
        List<Transaction> rtx = txs.subList(mid, txs.size());
        byte[] leftHash = buildMerkle(ltx);
        byte[] rightHash = buildMerkle(rtx);

        // 连接左右子树的哈希值并计算SHA-256哈希值
        return sha256(concatBytes(leftHash, rightHash));
    }

    public static String buildMerkleHex(List<Transaction> txs) {
        return bytesToHexString(buildMerkle(txs));
    }

    public static void waitCalculateMerkle() {
        sha256(concatBytes(RandomUtils.randomString(64).getBytes(StandardCharsets.UTF_8), hash2));
    }

}
