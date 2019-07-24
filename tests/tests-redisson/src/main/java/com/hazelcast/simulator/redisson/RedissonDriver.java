/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.redisson;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

import java.io.IOException;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;

public class RedissonDriver extends VendorDriver<RedissonClient> {

    private RedissonClient client;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        WorkerParameters params = new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("javaclient".equals(workerType)) {
            loadClientParameters(params);
        } else {
            throw new IllegalArgumentException(format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadClientParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", get("CLIENT_ARGS", ""))
                .set("nodes", fileAsText("nodes.txt"))
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    @Override
    public RedissonClient getVendorInstance() {
        return client;
    }

    @Override
    public void startVendorInstance() throws Exception {
        Config config = new Config();
        ClusterServersConfig serversConfig = config.useClusterServers();
        fillAddresses(serversConfig);

        if (get("REDIS_CLUSTER_PASSWORD") != null) {
            serversConfig.setPassword(get("REDIS_CLUSTER_PASSWORD"));
        }

        this.client = Redisson.create(config);

        System.out.println("Nodes in cluster : ");
        client.getClusterNodesGroup().getNodes().forEach(n-> {
            System.out.println(" Addr : " + n.getAddr() + " type : " + n.getType());
        });
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.shutdown();
        }
    }

    private void fillAddresses(ClusterServersConfig serversConfig) {
        String[] nodes = get("nodes").split(",");
        for (String node : nodes) {
            String[] addressParts = node.split(":");
            if (addressParts.length == 0 || addressParts.length > 2) {
                throw new IllegalArgumentException("Invalid node address. Example: localhost:11211");
            }

            serversConfig.addNodeAddress("redis://" + node);
        }
    }
}
