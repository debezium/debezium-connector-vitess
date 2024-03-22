/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class Gtid {

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

    Gtid(String transactionId) {
        initializeVersion(transactionId);
        parseGtid(transactionId);
    }

    private void parseGtid(String transactionId) {
        transactionId = trimVersion(transactionId);
        String[] transactions = transactionId.split(",");
        for (String transaction : transactions) {
            String[] parts = transaction.split(":");
            String hostname = parts[0];
            hosts.add(hostname);
            String maxSequenceValue = parts[1].split("-")[1];
            sequenceValues.add(maxSequenceValue);
        }
    }

    public boolean isHostSetEqual(Gtid hosts) {
        return this.hosts.equals(hosts.hosts);
    }

    public boolean isHostSetSupersetOf(Gtid previousHosts) {
        return this.hosts.containsAll(previousHosts.hosts);
    }

    public boolean isHostSetSubsetOf(Gtid previousHosts) {
        return previousHosts.hosts.containsAll(this.hosts);
    }
}
