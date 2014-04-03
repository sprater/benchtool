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

import java.util.concurrent.atomic.AtomicInteger;

import org.fcrepo.bench.BenchTool.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fcrepo.bench.BenchTool.Action.COMMIT_TX;
import static org.fcrepo.bench.BenchTool.Action.ROLLBACK_TX;
import static org.fcrepo.bench.BenchTool.Action.CREATE_TX;

/**
 * @author bbpennel
 * @date Feb 24, 2014
 */
public class TransactionState {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionState.class);

    private String transactionId;

    private int actionsAssigned;

    private int maxActions;

    private final AtomicInteger actionsCompleted;

    private boolean readyForCommit;

    public TransactionState(final int actionsPerTx) {
        transactionId = null;
        actionsAssigned = 0;
        this.actionsCompleted = new AtomicInteger(0);
        readyForCommit = false;
        maxActions = actionsPerTx;
    }

    /**
     * @param transactionId the transactionId to set
     */
    public void setTransactionId(final String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * @return the transactionId
     */
    public String getTransactionId() {
        return transactionId;
    }

    public void actionCompleted(final Action action) {
        if (action == CREATE_TX || action == COMMIT_TX || action == ROLLBACK_TX) {
            return;
        }
        final int completed = this.actionsCompleted.incrementAndGet();
        if (completed >= maxActions) {
            readyForCommit = true;
        }
        LOGGER.debug("Completed {} action(s) for {}", completed, transactionId);
    }

    /**
     * @return the actionsAssigned
     */
    public int getActionsAssigned() {
        return actionsAssigned;
    }

    /**
     * @param maxActions the maxActions to set
     */
    public void setMaxActions(final int maxActions) {
        this.maxActions = maxActions;
    }

    public void assignAction() {
        actionsAssigned++;
    }

    public boolean actionsAssigned() {
        return actionsAssigned > 0;
    }

    public boolean allActionsAssigned() {
        return actionsAssigned == maxActions && maxActions > 0;
    }

    public boolean transactionCreated() {
        return this.transactionId != null;
    }

    public void setReadyForCommit(final boolean readyForCommit) {
        this.readyForCommit = readyForCommit;
    }

    public boolean isReadyForCommit() {
        return readyForCommit;
    }
}
