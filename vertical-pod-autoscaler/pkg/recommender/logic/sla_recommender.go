/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logic

import (
	"fmt"
	"time"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

type slaPodResourceRecommender struct {
	slaDataProvider               SlaDataProvider
	vanillaPodResourceRecommender PodResourceRecommender
}

func (r *slaPodResourceRecommender) GetRecommendedPodResources(containerNameToAggregateStateMap model.ContainerNameToAggregateStateMap) RecommendedPodResources {
	for containerName := range containerNameToAggregateStateMap {
		// pass containerName (not pod name)
		data, err := r.slaDataProvider.GetSlaData(containerName, 1*time.Hour, 1*time.Minute)
		if err != nil {
			fmt.Printf("error fetching SLA data for %s: %v\n", containerName, err)
			continue
		}
		if len(data) == 0 {
			fmt.Printf("no SLA data for container %s\n", containerName)
			continue
		}
		printSlaData(data)
	}
	return r.vanillaPodResourceRecommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
}

func printSlaData(data []TimestampedSlaDataPoint) {
	fmt.Printf("\nSLA Data:\n")
	fmt.Printf("| %-30s | %-10s | %-10s |%-10s | %-10s |%-10s |\n", "timestamp", "cpu", "sla", "AvgRT", "BTCount", "Pods")
	fmt.Printf("| %-30s | %-10s | %-10s |%-10s | %-10s |%-10s |\n", "------------------------------", "----------", "----------", "----------", "----------", "----------")
	for _, dp := range data {
		fmt.Printf("| %-30s | %-10f | %-10f |%-10f | %-10f |%-10f |\n", dp.timestamp.Format(time.RFC3339), dp.point.Cpu, dp.point.Sla, dp.point.AvgRT, dp.point.BTCount, dp.point.Pods)
	}
}

func CreateSlaPodResourceRecommender(
	slaDataProvider SlaDataProvider,
	vanillaPodResourceRecommender PodResourceRecommender) PodResourceRecommender {

	return &slaPodResourceRecommender{
		slaDataProvider:               slaDataProvider,
		vanillaPodResourceRecommender: vanillaPodResourceRecommender,
	}
}
