package com.netflix.conductor.jedis;

import com.google.common.collect.Lists;

import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Provider;

public class TokenMapSupplierProvider implements Provider<TokenMapSupplier> {
    private final HostSupplier hostSupplier;

    private final DynomiteConfiguration configuration;

    @Inject
    public TokenMapSupplierProvider(HostSupplier hostSupplier, DynomiteConfiguration configuration) {
        this.hostSupplier = hostSupplier;
        this.configuration = configuration;
    }

    @Override
    public TokenMapSupplier get() {

        // If we specified the admin port for our dynomite servers, use that to get the hosts' tokens
        int dynomiteAdminPort = configuration.getDynomiteAdminPort();
        if(dynomiteAdminPort > -1) {
            return new DynomiteTokenMapSupplier(dynomiteAdminPort);
        }

        return new TokenMapSupplier() {

            HostToken token = new HostToken(1L, Lists.newArrayList(hostSupplier.getHosts()).get(0));

            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                return Arrays.asList(token);
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return token;
            }
        };
    }
}
