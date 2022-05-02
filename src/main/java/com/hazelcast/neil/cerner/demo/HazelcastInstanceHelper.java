/*
 * Copyright 2018-2021 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.neil.cerner.demo;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastInstanceHelper
{
    private static String ossMemberAddress = "127.0.0.1:5701";
    private static String entMemberAddress = "127.0.0.1:6701";
    private static String ossMapName = "appData";
    //private static String sourceTempMapName = "tmpAppData";
    private static String entMapName = "appData";
    //private static String targetTempMapName = "tmpAppData";
    private static String ossClusterGroupName = "dev";
    private static String entClusterGroupName = "enterprise";

    public static String getOssMapName()
    {
        return ossMapName;
    }

    public static String getEntMapName()
    {
        return entMapName;
    }

//    public static String getSourceTempMapName()
//    {
//        return sourceTempMapName;
//    }
//
//    public static String getTargetTempMapName()
//    {
//        return targetTempMapName;
//    }

    public static HazelcastInstance getOssHazelcastInstance() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(ossClusterGroupName);
        clientConfig.getNetworkConfig().addAddress(ossMemberAddress);
        HazelcastInstance hazelcastOssClientInstance = HazelcastClient.newHazelcastClient(clientConfig);
        return hazelcastOssClientInstance;
    }

    public static HazelcastInstance getEntHazelcastInstance() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(entClusterGroupName);
        clientConfig.getNetworkConfig().addAddress(entMemberAddress);
        HazelcastInstance hazelcastEntClientInstance = HazelcastClient.newHazelcastClient(clientConfig);
        return hazelcastEntClientInstance;
    }

    public static ClientConfig getOssClientConfig()
    {
        ClientConfig cfg = new ClientConfig();
        cfg.getNetworkConfig().addAddress(ossMemberAddress);
        cfg.setClusterName(ossClusterGroupName);
        return cfg;
    }

    public static ClientConfig getEntClientConfig()
    {
        ClientConfig cfg = new ClientConfig();
        cfg.getNetworkConfig().addAddress(entMemberAddress);
        cfg.setClusterName(entClusterGroupName);
        return cfg;
    }

    public static String getOssClusterGroupName()
    {
        return ossClusterGroupName;
    }

    public static String getEntClusterGroupName()
    {
        return entClusterGroupName;
    }
}
