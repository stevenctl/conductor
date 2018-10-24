package com.netflix.conductor.jedis;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.lb.HttpEndpointBasedTokenMapSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DynomiteTokenMapSupplier extends HttpEndpointBasedTokenMapSupplier {

    private static final Logger Logger = LoggerFactory.getLogger(DynomiteTokenMapSupplier.class);

    public DynomiteTokenMapSupplier(int port) {
        super("http://{hostname}:{port}/cluster_describe", port);
    }

    @Override
    public List<HostToken> getTokens(Set<Host> activeHosts) {

        // Doing this since not all tokens are received from an individual call
        // to a dynomite server
        // hence trying them all
        Set<HostToken> allTokens = new HashSet<HostToken>();

        for (Host host : activeHosts) {
            try {
                List<HostToken> hostTokens = parseTokenListFromJson(getTopologyJsonPayload((host.getHostAddress())));
                for (HostToken hToken : hostTokens) {
                    Logger.info("AUTOC3: in Abstract class, token = " + hToken.getToken() + " host = " + hToken.getHost().toString());
                    allTokens.add(hToken);
                }
            } catch (Exception e) {
                Logger.warn("Could not get json response for token topology [" + e.getMessage() + "]");
            }
        }
        return new ArrayList<>(allTokens);
    }

    /**
     * Parses a json payload like such:
     * {
     * "dcs": [
     * {
     * "name": "us-east-1",
     * "racks": [
     * {
     * "name": "us-east-1a",
     * "servers": [
     * {
     * "name": "0.0.0.0",
     * "host": "0.0.0.0",
     * "port": 8101,
     * "token": 222222222
     * }
     * ]
     * }
     * ]
     * }
     * ]
     * }
     *
     * @param json
     * @return
     */
    private List<HostToken> parseTokenListFromJson(String json) {

        List<HostToken> hostTokens = new ArrayList<>();

        ObjectMapper om = new ObjectMapper();
        try {
            JsonNode cluster = om.readTree(json);

            for (JsonNode dc : cluster.get("dcs")) {
                String dcName = dc.get("name").asText();
                for (JsonNode rack : dc.get("racks")) {
                    String rackName = rack.get("name").asText();
                    for (JsonNode server : rack.get("servers")) {
                        String serverName = server.get("name").asText();
                        String serverHost = server.get("name").asText();
                        int port = server.get("name").asInt();
                        long token = server.get("name").asLong();

                        Host host = new Host(serverName, serverHost, port, rackName, dcName, Status.Up);
                        HostToken hostToken = new HostToken(token, host);
                        hostTokens.add(hostToken);
                    }
                }
            }

        } catch (IOException e) {
            Logger.error("Failed to parse json response: " + json, e);
            throw new RuntimeException(e);
        }

        return hostTokens;

    }

}
