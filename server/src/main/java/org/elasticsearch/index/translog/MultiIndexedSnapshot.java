/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import java.io.Closeable;
import java.io.IOException;

/**
 * An indexed multi-snapshot providing random access to a collection of indexed snapshots.
 */
final class MultiIndexedSnapshot implements Translog.IndexedSnapshot {

    private final TranslogIndexedSnapshot[] snapshots;
    private final Closeable onClose;

    /**
     * Creates a new indexed multi-snapshot from the specified indexed snapshots.
     *
     * @param snapshots the indexed snapshots to wrap in this multi-snapshot
     * @param onClose resource to close when this snapshot is closed
     */
    public MultiIndexedSnapshot(final TranslogIndexedSnapshot[] snapshots, final Closeable onClose) {
        this.snapshots = snapshots;
        this.onClose = onClose;
    }

    @Override
    public Translog.Operation operation(final long seqNo) throws IOException {
        for (int i = snapshots.length - 1; i >= 0; i--) {
            final Translog.Operation operation = snapshots[i].operation(seqNo);
            if (operation != null) {
                return operation;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

}
