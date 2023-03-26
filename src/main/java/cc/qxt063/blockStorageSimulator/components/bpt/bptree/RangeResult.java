package cc.qxt063.blockStorageSimulator.components.bpt.bptree;

import java.util.LinkedList;

/**
 *
 * This is a simple wrapper class for our range queries where
 * we pack in a linked list all the matching results for easy
 * access and manipulation.
 *
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class RangeResult {

    // our linked list
    private final LinkedList<KVPair> queryResult;

    /**
     * Constructor that instantiates basically our linked list
     */
    public RangeResult()
        {this.queryResult = new LinkedList<>();}

    /**
     * Used to give us access to the actual list
     *
     * @return query result list reference
     */
    public LinkedList<KVPair> getQueryResult()
        {return(queryResult);}
}
