package org.elasticsearch.index.seqno;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class RetentionLeaseAlreadyExistsException extends ResourceAlreadyExistsException {

    public RetentionLeaseAlreadyExistsException(final String id) {
        super("retention lease with ID [" + id + "] already exists");
    }

    public RetentionLeaseAlreadyExistsException(StreamInput in) throws IOException {
        super(in);
    }

}
