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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.neil.cerner.data.ApplicationData;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class creates Jet pipelines to move data between two clusters.  There are three options for how it will move
 * the data:
 *      existingOnly - only move existing data from the open source cluster to the enterprise cluster
 *      newOnly - only sync new records that come in between the two clusters, but will not sync existing data in either
 *      both - will move existing data from the opern source cluster to the enterprise and sync new records in either
 *              cluster to the other
 *
 * @author Neil Stickels
 * @version 1.0
 *
 */
public class JetBridgeOSAndEnt
{
    private static ILogger logger;
    private static JetInstance jet;
    private static final String changeEventMapName = "changeEventMap";

    /**
     * Jet processes must implement the main(String[] args) and define what the process will do.  main() expects exactly
     * one string argument which is what type of syncing this process will do.  Main will create remote connections
     * to both the open source and the enterprise clusters.  If either newOnly or both option is selected, it will
     * create an IMap on the Jet instance for holding the data to transfer from one cluster to the other.  It will also
     * create an EntryListener on the IMap for both the open source and enterprise cluster to write changes to this
     * local IMap, which the Jet pipeline will then sync to the other cluster appropriately.  If existingOnly is
     * chosen, the EventListeners and local IMap are not created.  If either existingOnly or both option is selected,
     * it will create a separate pipeline which will move all the existing data from the open source map to the
     * enterprise map.  This pipeline runs once on all the existing data and then completes
     * @param args One of three args is required:
     *              existingOnly - only move existing data from the open source cluster to the enterprise cluster
     *              newOnly - only sync new records that come in between the two clusters, but will not sync existing data in either
     *              both - will move existing data from the opern source cluster to the enterprise and sync new records in either
     *                cluster to the other
     */
    public static void main(String[] args) {
        logger = Logger.getLogger(JetBridgeOSAndEnt.class);
        checkArgs(args);
        String arg = args[0];
        jet = Jet.newJetInstance(getJetConfig());
        HazelcastInstance oss = HazelcastInstanceHelper.getOssHazelcastInstance();
        HazelcastInstance ent = HazelcastInstanceHelper.getEntHazelcastInstance();
        if(arg.equals("both") || arg.equals("newOnly"))
        {
            createNewMap(oss, HazelcastInstanceHelper.getOssMapName(), ent, HazelcastInstanceHelper.getEntMapName());
            submitNewJob();
        }
        if(arg.equals("both") || arg.equals("existingOnly"))
            submitExistingJob();
    }

    /**
     * Used to make sure one of the three required args is present.  If they are not, the program will abort with an
     * error message specifying which args are required.
     * @param args
     */
    private static void checkArgs(String [] args) {
        // Check how many arguments were passed in and that it is a correct argument
        if(args.length != 1)
        {
            showArgsError();
        } else
        {
            String arg = args[0];
            if(!arg.equals("existingOnly") && !arg.equals("newOnly") && !arg.equals("both"))
                showArgsError();
        }
    }

    private static void showArgsError()
    {
        System.out.println("Proper Usage is: java com.hazelcast.neil.cerner.demo.JetBridgeOSAndEnt [whichJob]");
        System.out.println("  where [whichJob] is one of: existingOnly, newOnly or both");
        System.exit(0);
    }

    /**
     * This creates a Jet job to write the existing open source data to enterprise.  This will only be called if the
     * existingOnly or both argument is provided.
     */
    private static void submitExistingJob() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Sync Existing OSS to Ent");
        jobConfig.setSnapshotIntervalMillis(10000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jet.newJob(buildExistingOSSToEntPipeline(),jobConfig);
    }

    /**
     * This creates a Jet job to synchronize new data in either open source cluster or enterprise cluster to the other.
     * This will only be called if the newOnly or both argument is provided.
     */
    private static void submitNewJob() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Sync New data between OSS and Ent");
        jobConfig.setSnapshotIntervalMillis(10000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jet.newJob(buildSyncingPipeline(),jobConfig);
    }

    /**
     * Creates a new pipeline which reads all the existing data in the open source map and writes it to the
     * enterprise map
     * @return the created pipeline
     */
    private static Pipeline buildExistingOSSToEntPipeline() {

        Pipeline p = Pipeline.create();

        //Consume existing messages from open source Map and copy to enterprise Map
        p.readFrom(Sources.remoteMap(HazelcastInstanceHelper.getOssMapName(), HazelcastInstanceHelper.getOssClientConfig()))
                .setName("read from open source "+HazelcastInstanceHelper.getOssMapName())
                //.drainTo(Sinks.logger());
                .writeTo(Sinks.remoteMap(HazelcastInstanceHelper.getEntMapName(), HazelcastInstanceHelper.getEntClientConfig())).setName("write to enterprise "+HazelcastInstanceHelper.getEntMapName());
        return p;
    }

    /**
     * Creates a pipeline to read any events on the local changeEventMap and determine which cluster this event should
     * be written to.  If the event came from the enterprise cluster, it will write it to the open source and vice versa.
     * @return the Jet Pipeline for this process
     */
    private static Pipeline buildSyncingPipeline() {

        Pipeline p = Pipeline.create();
        IMap<String, ApplicationData> changeEventMap = jet.getMap(changeEventMapName);

        //Consume existing messages from changeEventMap
        StreamStage<Map.Entry<String, ApplicationData>> changeEvents = p.readFrom(Sources.mapJournal(changeEventMap, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps().setName("Receive Change Map Event");
        //Determine where the event should go to
        StreamStage<Map.Entry<String, ApplicationData>> filterEntEvents = changeEvents.filter(entry ->
        {
            String sKey = entry.getKey();
            StringTokenizer st = new StringTokenizer(sKey,"-");
            String cluster = st.nextToken();
            boolean fromOss = cluster.equals(HazelcastInstanceHelper.getOssClusterGroupName());
            return fromOss;
        }).setName("Filter only events to Enterprise");
        StreamStage<Map.Entry<String, ApplicationData>> filterOssEvents = changeEvents.filter(entry ->
        {
            String sKey = entry.getKey();
            StringTokenizer st = new StringTokenizer(sKey,"-");
            String cluster = st.nextToken();
            boolean fromEnt = cluster.equals(HazelcastInstanceHelper.getEntClusterGroupName());
            return fromEnt;
        }).setName("Filter only events to Open Source");
        // change to event that target Map will take
        StreamStage<Map.Entry<Integer, ApplicationData>> toEntEvents = filterEntEvents.map(entry ->
            {
                String sKey = entry.getKey();
                StringTokenizer st = new StringTokenizer(sKey,"-");
                String cluster = st.nextToken();
                Integer iKey = Integer.parseInt(st.nextToken());
                Map.Entry<Integer, ApplicationData> newEntry = Util.entry(iKey, entry.getValue());
                return newEntry;
            }).setName("Create events to send to Enterprise");
        StreamStage<Map.Entry<Integer, ApplicationData>> toOssEvents = filterOssEvents.map(entry ->
            {
                String sKey = entry.getKey();
                StringTokenizer st = new StringTokenizer(sKey,"-");
                String cluster = st.nextToken();
               Integer iKey = Integer.parseInt(st.nextToken());
               Map.Entry<Integer, ApplicationData> newEntry = Util.entry(iKey, entry.getValue());
               return newEntry;
            }).setName("Entries to Send to OSS");
        // write out the events to the appropriate map
        SinkStage writeToEnt = toEntEvents.writeTo(Sinks.remoteMap(HazelcastInstanceHelper.getEntMapName(), HazelcastInstanceHelper.getEntClientConfig()))
                .setName("Write events to Enterprise");
        SinkStage writeToOss = toOssEvents.writeTo(Sinks.remoteMap(HazelcastInstanceHelper.getOssMapName(), HazelcastInstanceHelper.getOssClientConfig()))
                .setName("Write events to Open Source");
        return p;
    }

    /**
     * This will create a new Journaled IMap on the local Jet instance, as well as creating EventListeners on both the
     * open source IMap and enterprise IMap to make sure changes to either are written to this local IMap.  The Jet
     * process buildSyncingPipeline will use this local IMap to push changes to the appropriate cluster.
     * @param oss - HazelcastInstance for the open source cluster
     * @param ossMapName - Open source cluster IMap name
     * @param ent - HazelcastInstance for the enterprise cluster
     * @param entMapName - Enterprice cluster IMap name
     */
    private static void createNewMap(HazelcastInstance oss, String ossMapName, HazelcastInstance ent, String entMapName)
    {
        EventJournalConfig eventConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(5000)
                .setTimeToLiveSeconds(20);
        MapConfig newMapConfig = new MapConfig();
        newMapConfig.setName(changeEventMapName);
        newMapConfig.setTimeToLiveSeconds(5);
        newMapConfig.setEventJournalConfig(eventConfig);
        jet.getConfig().getHazelcastConfig().addMapConfig(newMapConfig);
        IMap<String, ApplicationData> changeEventMap = jet.getMap(changeEventMapName);
        MyMapListener mml = new MyMapListener(ossMapName, HazelcastInstanceHelper.getOssClusterGroupName(), changeEventMap);
        oss.getMap(ossMapName).addEntryListener(mml, true);
        MyMapListener mml2 = new MyMapListener(entMapName, HazelcastInstanceHelper.getEntClusterGroupName(), changeEventMap);
        ent.getMap(entMapName).addEntryListener(mml2, true);
    }

    private static JetConfig getJetConfig() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setClusterName("jet");
        return jetConfig;
    }

}
