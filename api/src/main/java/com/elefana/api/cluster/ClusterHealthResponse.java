/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.api.cluster;

import com.elefana.api.ApiResponse;
import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ClusterHealthResponse extends ApiResponse {
	@JsonProperty("cluster_name")
	private String clusterName;
	@JsonProperty("status")
	private String status;
	@JsonProperty("timed_out")
	private boolean timedOut;
	@JsonProperty("number_of_nodes")
	private int numberOfNodes;
	@JsonProperty("number_of_data_nodes")
	private int numberOfDataNodes;
	@JsonProperty("active_primary_shards")
	private int activePrimaryShards;
	@JsonProperty("active_shards")
	private int activeShards;
	@JsonProperty("relocating_shards")
	private int relocatingShards;
	@JsonProperty("initializing_shards")
	private int initializingShards;
	@JsonProperty("unassigned_shards")
	private int unassignedShards;
	@JsonProperty("delayed_unassigned_shards")
	private int delayedUnassignedShards;
	@JsonProperty("number_of_pending_tasks")
	private int numberOfPendingTasks;
	@JsonProperty("number_of_in_flight_fetch")
	private int numberOfInFlightFetch;
	@JsonProperty("task_max_waiting_in_queue_millis")
	private long taskMaxWaitingInQueueMillis;
	@JsonProperty("active_shards_percent_as_number")
	private double activeShardsPercentAsNumber;
	

	public ClusterHealthResponse() {
		super(HttpResponseStatus.OK.code());
	}

	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(this);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean isTimedOut() {
		return timedOut;
	}

	public void setTimedOut(boolean timedOut) {
		this.timedOut = timedOut;
	}

	public int getNumberOfNodes() {
		return numberOfNodes;
	}

	public void setNumberOfNodes(int numberOfNodes) {
		this.numberOfNodes = numberOfNodes;
	}

	public int getNumberOfDataNodes() {
		return numberOfDataNodes;
	}

	public void setNumberOfDataNodes(int numberOfDataNodes) {
		this.numberOfDataNodes = numberOfDataNodes;
	}

	public int getActivePrimaryShards() {
		return activePrimaryShards;
	}

	public void setActivePrimaryShards(int activePrimaryShards) {
		this.activePrimaryShards = activePrimaryShards;
	}

	public int getActiveShards() {
		return activeShards;
	}

	public void setActiveShards(int activeShards) {
		this.activeShards = activeShards;
	}

	public int getRelocatingShards() {
		return relocatingShards;
	}

	public void setRelocatingShards(int relocatingShards) {
		this.relocatingShards = relocatingShards;
	}

	public int getInitializingShards() {
		return initializingShards;
	}

	public void setInitializingShards(int initializingShards) {
		this.initializingShards = initializingShards;
	}

	public int getUnassignedShards() {
		return unassignedShards;
	}

	public void setUnassignedShards(int unassignedShards) {
		this.unassignedShards = unassignedShards;
	}

	public int getDelayedUnassignedShards() {
		return delayedUnassignedShards;
	}

	public void setDelayedUnassignedShards(int delayedUnassignedShards) {
		this.delayedUnassignedShards = delayedUnassignedShards;
	}

	public int getNumberOfPendingTasks() {
		return numberOfPendingTasks;
	}

	public void setNumberOfPendingTasks(int numberOfPendingTasks) {
		this.numberOfPendingTasks = numberOfPendingTasks;
	}

	public int getNumberOfInFlightFetch() {
		return numberOfInFlightFetch;
	}

	public void setNumberOfInFlightFetch(int numberOfInFlightFetch) {
		this.numberOfInFlightFetch = numberOfInFlightFetch;
	}

	public long getTaskMaxWaitingInQueueMillis() {
		return taskMaxWaitingInQueueMillis;
	}

	public void setTaskMaxWaitingInQueueMillis(long taskMaxWaitingInQueueMillis) {
		this.taskMaxWaitingInQueueMillis = taskMaxWaitingInQueueMillis;
	}

	public double getActiveShardsPercentAsNumber() {
		return activeShardsPercentAsNumber;
	}

	public void setActiveShardsPercentAsNumber(double activeShardsPercentAsNumber) {
		this.activeShardsPercentAsNumber = activeShardsPercentAsNumber;
	}
}
