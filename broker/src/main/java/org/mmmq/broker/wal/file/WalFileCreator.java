package org.mmmq.broker.wal;

interface WalFileCreator {

    WalFile create(String topicName, int index);
}
