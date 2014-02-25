/**
 * Copyright 2013 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.bench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.fcrepo.bench.BenchTool.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author bbpennel
 * @date Feb 24, 2014
 */
public class TransactionManager {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(TransactionManager.class);

    public static enum TransactionMode {
        NONE, COMMIT, ROLLBACK
    };

    private final TransactionMode mode;

    private final int actionsPerTx;

    private final List<Transaction> transactions;

    private int nextTxIndex;

    private final int parallelTx;

    private long createTime;

    private long commitTime;

    public TransactionManager(final TransactionMode mode, final int actionsPerTx,
            final int parallelTx) throws IOException {
        this.mode = mode;
        this.actionsPerTx = actionsPerTx;
        this.parallelTx = parallelTx;
        this.nextTxIndex = 0;

        this.transactions = new ArrayList<>(parallelTx);
    }

    public Transaction getTransaction() throws IOException {
        final Transaction tx;

        if (transactions.size() < parallelTx) {
            tx = new Transaction(this);
            transactions.add(tx);
            return tx;
        }

        tx = transactions.get(nextTxIndex);

        if (tx.isReadyForCommit()
                || (actionsPerTx > 0 && tx.getActionsAssigned() == actionsPerTx - 1)) {
            // Transaction is at capacity, remove it from the list of candidates
            transactions.remove(nextTxIndex);
        } else {
            // Move to the next transaction
            nextTxIndex = (nextTxIndex + 1) % parallelTx;
        }

        return tx;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void clearTransactions() {
        transactions.clear();
    }

    /**
     * @return the actionsPerTx
     */
    public int getActionsPerTx() {
        return actionsPerTx;
    }

    /**
     * @return the parallelTx
     */
    public int getParallelTx() {
        return parallelTx;
    }

    /**
     * @return the mode
     */
    public TransactionMode getMode() {
        return mode;
    }

    public Action getFinalizeAction() {
        if (mode == TransactionMode.COMMIT) {
            return Action.COMMIT_TX;
        }
        return Action.ROLLBACK_TX;
    }


    /**
     * @return the createTime
     */
    public long getCreateTime() {
        return createTime;
    }

    public void addToCreateTime(final long time) {
        createTime += time;
    }

    /**
     * @return the commitTime
     */
    public long getCommitTime() {
        return commitTime;
    }

    public void addToCommitTime(final long time) {
        commitTime += time;
    }
}
