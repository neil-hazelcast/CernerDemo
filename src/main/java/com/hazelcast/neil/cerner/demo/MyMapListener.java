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
 *
 */

package com.hazelcast.neil.cerner.demo;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.neil.cerner.data.ApplicationData;


/**
 * MyMapListener extends the Hazelcast Map EntryListener interface.  It is used to help synchronize
 * map entries between two Hazelcast clusters.  This class isn't meant to be long running, but rather
 * to be used temporarily while migrating from one Hazelcast cluster to another, but when you have to
 * keep changes to either cluster in sync with the other during the migration.  It assumes there is a
 * middle-man map which changes to the existing map will be written to.  A Jet process will then listen
 * to the middle-man map and push events from the source cluster to the target cluster.
 * @author Neil Stickels
 * @version 1.0
 */
public class MyMapListener implements EntryListener<Integer, ApplicationData>
{
    private IMap<String, ApplicationData> changeEventMap;
    private String thisMapName;
    private String clusterName;
    private static ILogger logger;

    /**
     * The constructor for the Listener
     * @param thisMapName - The name of the IMap this cluster is listening to.  Just used for logging
     * @param clusterName - The name of the cluster this IMap is running in.  This is used to designate where the
     *                    entry came from by the Jet process
     * @param changeEventMap - The IMap that changes to this IMap should be written to
     */
    public MyMapListener(String thisMapName, String clusterName, IMap<String, ApplicationData> changeEventMap)
    {
        this.thisMapName = thisMapName;
        this.clusterName = clusterName;
        this.changeEventMap = changeEventMap;
        logger = Logger.getLogger(MyMapListener.class);
    }

    /**
     * When a new Entry is added to this IMap, this will check to see if that Entry already exists in the changeEventMap.
     * If the Entry doesn't exist, it will add it to the changeEventMap, but will first change the key of the entry to
     * prepend which cluster this Map is part of as part of the key on the changeEventMap.  If the Entry does already
     * exist, it will determine if this Event is the same or newer than the one in changeEventMap.  If it is the same,
     * nothing is done.  If it is newer, it will write the change to the changeEventMap.  If it is older, it will
     * log a warning.
     * @param entryEvent
     * @see EntryListener
     */
    @Override
    public void entryAdded(EntryEvent<Integer, ApplicationData> entryEvent)
    {
        Integer iKey = entryEvent.getKey();
        String sKey = clusterName+"-"+iKey;
        ApplicationData newValue = entryEvent.getValue();
        long newTime = newValue.getLastUpdatedNanoTime();
        if(changeEventMap.containsKey(sKey))
        {
            ApplicationData existingValue = changeEventMap.get(sKey);
            if(existingValue.getGuid().equals(newValue.getGuid()))
            {
                // do nothing
            } else
            {
                long existingTime = existingValue.getLastUpdatedNanoTime();
                // this means that the new Entry added already exists with a newer version on the changeEventMap
                // right now, it is just logging a warning.  If a programmatic change is required, it could be added here
                if(existingTime > newTime)
                    logger.warning("new value added in "+thisMapName+" with a newer different value in "+changeEventMap.getName());
                else
                    changeEventMap.put(sKey,newValue);
            }
        } else
        {
            changeEventMap.put(sKey,newValue);
        }
    }

    @Override
    public void entryEvicted(EntryEvent<Integer, ApplicationData> entryEvent)
    {

    }

    @Override
    public void entryExpired(EntryEvent<Integer, ApplicationData> entryEvent)
    {

    }

    @Override
    public void entryRemoved(EntryEvent<Integer, ApplicationData> entryEvent)
    {

    }
    /**
     * When an Entry is updated in this IMap, this will check to see if that Entry already exists in the changeEventMap.
     * If the Entry doesn't exist, it will add it to the changeEventMap, but will first change the key of the entry to
     * prepend which cluster this Map is part of as part of the key on the changeEventMap. If the Entry does already
     * exist, it will determine if this Event is the same or newer than the one in changeEventMap.  If it is the same,
     * nothing is done.  If it is newer, it will write the change to the changeEventMap.  If it is older, it will
     * log a warning.
     * @param entryEvent
     * @see EntryListener
     */
    @Override
    public void entryUpdated(EntryEvent<Integer, ApplicationData> entryEvent)
    {
        Integer iKey = entryEvent.getKey();
        String sKey = clusterName+"-"+iKey;
        ApplicationData newValue = entryEvent.getValue();
        long newTime = newValue.getLastUpdatedNanoTime();
        if(changeEventMap.containsKey(sKey))
        {
            ApplicationData existingValue = changeEventMap.get(sKey);
            if(existingValue.getGuid().equals(newValue.getGuid()))
            {
                // do nothing
            } else
            {
                long existingTime = existingValue.getLastUpdatedNanoTime();
                // this means that the new Entry added already exists with a newer version on the changeEventMap
                // right now, it is just logging a warning.  If a programmatic change is required, it could be added here
                if(existingTime > newTime)
                    logger.warning("new value changed in "+thisMapName+" with a newer different value in "+changeEventMap.getName());
                else
                    changeEventMap.put(sKey,newValue);
            }
        } else
        {
            changeEventMap.put(sKey,newValue);
        }
    }

    @Override
    public void mapCleared(MapEvent mapEvent)
    {

    }

    @Override
    public void mapEvicted(MapEvent mapEvent)
    {

    }
}
