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
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

type slaPodResourceRecommender struct {
	slaDataProvider               SlaDataProvider
	vanillaPodResourceRecommender PodResourceRecommender
}

func (r *slaPodResourceRecommender) GetRecommendedPodResources(containerNameToAggregateStateMap model.ContainerNameToAggregateStateMap) RecommendedPodResources {
	aggregatedData := []TimestampedSlaDataPoint{}
	for containerName := range containerNameToAggregateStateMap {
		// pass containerName (not pod name)
		data, err := r.slaDataProvider.GetSlaData(containerName, 2*time.Hour, 1*time.Minute)
		if err != nil {
			fmt.Printf("error fetching SLA data for %s: %v\n", containerName, err)
			continue
		}
		if len(data) == 0 {
			fmt.Printf("no SLA data for container %s\n", containerName)
			continue
		}
		fmt.Printf("\nSLA Data for %s:\n", containerName)
		printSlaData(data)
		if strings.Contains(containerName, "demo1-") {
			aggregatedData = append(aggregatedData, data...)
		}
	}
	//printSlaData(aggregatedData)
	fmt.Println("\nSLA Data for demo Aggregated")
	printSlaData(calculateSlaBasedRecommendation(aggregatedData))
	return r.vanillaPodResourceRecommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
}

func printSlaData(data []TimestampedSlaDataPoint) {
	fmt.Printf("| %-30s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s |%-10s |\n", "timestamp", "cpu", "avgCpu", "Memory", "sla", "AvgRT", "RT", "BTCount", "Pods")
	fmt.Printf("| %-30s | %-10s | %-10s | %-10s | %-10s |%-10s | %-10s | %-10s |%-10s |\n", "---------------------------", "----------", "----------", "---------------", "----------", "----------", "----------", "----------", "----------")
	for _, dp := range data {
		fmt.Printf("| %-30s | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f |\n", dp.timestamp.Format(time.RFC3339), dp.point.Cpu, dp.point.AvgCpu, dp.point.Memory, dp.point.Sla, dp.point.AvgRT, dp.point.RT, dp.point.BTCount, dp.point.Pods)
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

// type RecommendedContainerResources struct {
// 	// Recommended optimal amount of resources.
// 	Target model.Resources
// 	// Recommended minimum amount of resources.
// 	LowerBound model.Resources
// 	// Recommended maximum amount of resources.
// 	UpperBound model.Resources
// }

func calculateSlaBasedRecommendation(data []TimestampedSlaDataPoint) []TimestampedSlaDataPoint { //(cpuRecommendation, memoryRecommendation float64) {
	// // aggregate data
	slices.SortFunc(data, func(a, b TimestampedSlaDataPoint) int {
		return cmp.Compare(a.point.Cpu, b.point.Cpu)
	})

	data = slices.DeleteFunc(data, func(dp TimestampedSlaDataPoint) bool {
		return dp.point.Cpu == 0.0 || math.IsNaN(dp.point.Cpu) || dp.point.AvgCpu == 0.0 || dp.point.Pods < 1 || dp.point.BTCount == 0.0 || math.IsNaN(dp.point.RT)
	})

	// 	// remove outliers
	// 	// aggregate by range
	slices.SortFunc(data, func(a, b TimestampedSlaDataPoint) int {
		return cmp.Compare(a.point.Cpu, b.point.Cpu)
	})
	// 	// create mapping

	return data
}
