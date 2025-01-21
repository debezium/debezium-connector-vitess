/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.debezium.DebeziumException;

public class Gtid {

    public String getVersion() {
        return version;
    }

    private String version = "";

    public Set<String> getHosts() {
        return hosts;
    }

    private Set<String> hosts = new HashSet();

    public List<String> getSequenceValues() {
        return sequenceValues;
    }

    private List<String> sequenceValues = new ArrayList();

    private static final String PREFIX_LAST_CHAR = "/";

    private static int getVersionEndIndex(String transactionId) {
        return transactionId.indexOf(PREFIX_LAST_CHAR);
    }

    private static String trimVersion(String transactionId) {
        int index = getVersionEndIndex(transactionId);
        if (index != -1) {
            return transactionId.substring(index + 1);
        }
        return transactionId;
    }

    private void initializeVersion(String transactionId) {
        int index = getVersionEndIndex(transactionId);
        if (index != -1) {
            this.version = transactionId.substring(0, index);
        }
    }

    public Gtid(String transactionId) {
        try {
            initializeVersion(transactionId);
            parseGtid(transactionId);
        }
        catch (Exception e) {
            throw new DebeziumException("Error parsing GTID: " + transactionId, e);
        }
    }

    private void parseGtid(String transactionId) {
        transactionId = trimVersion(transactionId);
        String[] transactions = transactionId.split(",");
        for (String transaction : transactions) {
            String[] hostAndPositions = transaction.split(":");
            String hostname = hostAndPositions[0];
            hosts.add(hostname);
            // This is either a range format eg 1-10 or a single position eg 8, either case we want the last number
            String[] positions = hostAndPositions[1].split("-");
            String maxSequenceValue = positions[positions.length - 1];
            sequenceValues.add(maxSequenceValue);
        }
    }

    public boolean isHostSetSupersetOf(Gtid otherHosts) {
        return this.hosts.containsAll(otherHosts.hosts);
    }

    @Override
    public String toString() {
        return "Gtid{"
                + "versions="
                + version
                + ", hosts="
                + hosts
                + ", sequenceValues="
                + sequenceValues
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Gtid gtid = (Gtid) o;
        return Objects.equals(version, gtid.version) &&
                Objects.equals(hosts, gtid.hosts) &&
                Objects.equals(sequenceValues, gtid.sequenceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, hosts, sequenceValues);
    }
}
