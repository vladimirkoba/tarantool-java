package org.tarantool.protocol;

public class TarantoolGreeting {
    private final String serverVersion;

    public TarantoolGreeting(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }
}
