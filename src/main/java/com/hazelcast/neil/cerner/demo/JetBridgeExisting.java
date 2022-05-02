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
//import com.hazelcast.core.IQueue;
//import com.hazelcast.demo.util.QueueRestClientUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public class JetBridgeExisting
{
    private static ILogger logger;
    private static  JetInstance jet;

    public static void main(String[] args) {
        logger = Logger.getLogger(JetBridgeExisting.class);
        jet = Jet.newJetInstance(getJetConfig());
        submitJob();
    }

    private static void submitJob() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(HazelcastInstanceHelper.getOssMapName()+"MapExistingMessageBridge");
        jobConfig.setSnapshotIntervalMillis(10000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jet.newJob(buildPipeline(),jobConfig).join();
    }

    private static Pipeline buildPipeline() {

        Pipeline p = Pipeline.create();

        //Consume existing messages from source Map
        p.readFrom(Sources.remoteMap(HazelcastInstanceHelper.getOssMapName(), HazelcastInstanceHelper.getOssClientConfig()))
                .setName("read from sourceMap")
                //.drainTo(Sinks.logger());
                .writeTo(Sinks.remoteMap(HazelcastInstanceHelper.getEntMapName(), HazelcastInstanceHelper.getEntClientConfig())).setName("write to targetMap");
        return p;
    }

    private static JetConfig getJetConfig() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setClusterName("jet");
        return jetConfig;
    }

}

