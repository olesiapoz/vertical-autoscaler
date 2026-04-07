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
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

const RESPONSE_TIME_THRESHOLD = 3.0

type slaPodResourceRecommender struct {
	slaDataProvider               SlaDataProvider
	vanillaPodResourceRecommender PodResourceRecommender
}

type CPURangeSLA struct {
	RangeID int
	SLA     float64
}

func (r *slaPodResourceRecommender) GetRecommendedPodResources(containerNameToAggregateStateMap model.ContainerNameToAggregateStateMap) RecommendedPodResources {
	aggregatedData := []TimestampedSlaDataPoint{}

	vpaRecommendationsLimits := r.vanillaPodResourceRecommender.GetRecommendedPodResources(containerNameToAggregateStateMap)

	for containerName := range containerNameToAggregateStateMap {
		// pass containerName (not pod name)
		data, err := r.slaDataProvider.GetSlaData(containerName, 5*time.Hour, 1*time.Minute)
		if err != nil {
			fmt.Printf("error fetching SLA data for %s: %v\n", containerName, err)
			continue
		}
		if len(data) == 0 {
			fmt.Printf("no SLA data for container %s\n", containerName)
			continue
		}
		// fmt.Printf("\nSLA Data for %s:\n", containerName)
		// printSlaData(data)
		if strings.Contains(containerName, "demo1") {
			aggregatedData = append(aggregatedData, data...)
		}
	}
	//printSlaData(aggregatedData)
	// ... after fetching data from Prometheus
	if len(aggregatedData) > 0 {
		fmt.Println("\nSLA Data for demo Aggregated")
		//printSlaData(calculateSlaBasedRecommendation(aggregatedData))
		if rec, ok := vpaRecommendationsLimits["demo1"]; ok && rec.Target != nil {
			fmt.Printf("Recommended CPU limits from vanilla recommender: %d millicores\n",
				rec.Target[model.ResourceCPU])

			appendSlaDataToCSV("/tmp/sla_results.csv", aggregatedData, int64(rec.Target[model.ResourceCPU]))
		}
	}
	if len(aggregatedData) > 0 {
		// cpuRangesSLA := returnSLARanges(aggregatedData)
		// fmt.Println("SLA Range")
		// fmt.Println(cpuRangesSLA)

		cpuRangesSLA := returnSLARanges(aggregatedData)
		fmt.Printf("Aggregated data values: %d\n", len(aggregatedData))
		smoothedRangesSLA := smoothValuesBySMA(cpuRangesSLA)
		fmt.Println("Smoothed SLA Range")
		fmt.Println(smoothedRangesSLA)

		threshhold := 0
		for _, s := range smoothedRangesSLA {
			if s.SLA >= 99.0 {
				if threshhold < s.RangeID {
					threshhold = s.RangeID
				}
			}
		}

		if len(aggregatedData) > 0 {
			lastPoint := aggregatedData[len(aggregatedData)-3]

			// CRITICAL FIX: Check if 'point' itself is nil
			if lastPoint.point != nil {
				cpuLimit := lastPoint.point.CPULimits
				fmt.Printf("Recommended CPU threshold for SLA: %.2f%% is %d and recomended Limits %.2f millicores, while current %.2f\n",
					99.0, threshhold, cpuLimit/(float64(threshhold)/100.0)*1000, cpuLimit)
			} else {
				fmt.Println("[WARN] Last data point exists but the 'point' metrics are nil")
			}
		} else {
			fmt.Println("[WARN] No aggregated data available for threshold calculation")
		}
	}

	// 2. Get the vanilla recommendations

	// 3. SAFETY CHECK: Ensure "demo1" exists in the result map before accessing it
	if rec, ok := vpaRecommendationsLimits["demo1"]; ok && rec.Target != nil {
		fmt.Printf("Recommended CPU limits from vanilla recommender: %d millicores\n",
			rec.Target[model.ResourceCPU])
	}

	return vpaRecommendationsLimits
	//return r.vanillaPodResourceRecommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
}

func printSlaData(data []TimestampedSlaDataPoint) {
	fmt.Printf("| %-30s | %-10s | %-10s | %-10s | %-10s |%-10s | %-10s | %-10s | %-10s | %-10s | %-10s |%-10s |\n", "timestamp", "cpuLimits", "cpu", "maxCpu", "avgCpu", "MemoryLimits", "Memory", "sla", "AvgRT", "RT", "BTCount", "Pods")
	fmt.Printf("| %-30s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s |%-10s | %-10s | %-10s |%-10s |\n", "---------------------------", "----------", "----------", "----------", "----------", "----------", "---------------", "----------", "----------", "----------", "----------", "----------")
	for _, dp := range data {
		fmt.Printf("| %-30s | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f | %-10f |\n", dp.timestamp.Format(time.RFC3339), dp.point.CPULimits, dp.point.Cpu, dp.point.MaxCpu, dp.point.AvgCpu, dp.point.MemoryLimits, dp.point.Memory, dp.point.Sla, dp.point.AvgRT, dp.point.RT, dp.point.BTCount, dp.point.Pods)
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

	data = FilterOutliers(data)

	// 	// aggregate by range
	if len(data) == 0 {
		return []TimestampedSlaDataPoint{}
	}

	slices.SortFunc(data, func(a, b TimestampedSlaDataPoint) int {
		return cmp.Compare(a.point.Cpu, b.point.Cpu)
	})

	// create mapping
	return data
}

// func returnSLARanges(data []TimestampedSlaDataPoint) []CPURangeSLA {
// 	min := math.Round(slices.MinFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	}).point.Cpu)
// 	max := math.Round(slices.MaxFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	}).point.Cpu)
// 	var cpuRangesSLA []CPURangeSLA

// 	for i := min; i <= max; i += 3 {
// 		end := i + 3

// 		if end > max+1 {
// 			end = max + 1
// 		}

// 		chunk := make([]TimestampedSlaDataPoint, 0)
// 		for _, s := range data {
// 			if s.point.Cpu >= i && s.point.Cpu <= end {
// 				chunk = append(chunk, s)
// 			}
// 		}

// 		fmt.Println("\n Chunk Data for demo")
// 		printSlaData(chunk)
// 		if len(chunk) > 0 {
// 			cpuRange := calculateRangeSLA(chunk, 3.0)
// 			cpuRangesSLA = append(cpuRangesSLA, cpuRange)
// 		}

// 	}

// 	return cpuRangesSLA
// }

// func calculateRangeSLA(data []TimestampedSlaDataPoint, SlaRT float64) CPURangeSLA {
// 	rangeID := slices.MaxFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	})
// 	maxID := int(math.Round(rangeID.point.Cpu))
// 	dev := len(data)
// 	div := slices.DeleteFunc(data, func(data TimestampedSlaDataPoint) bool {
// 		return data.point.RT >= SlaRT
// 	})

// 	rangeSLA := (float64(len(div)) / float64(dev)) * 100 / 100.0

// 	if float64(dev) > 5 {
// 		fmt.Printf("Range: %d, SLA: %.2f%% dev: %d , div: %d \n", maxID, rangeSLA*100, dev, len(div))
// 	}
// 	return CPURangeSLA{
// 		RangeID: maxID,
// 		SLA:     rangeSLA * 100.0,
// 	}
// }

// func returnSLARanges(data []TimestampedSlaDataPoint) []CPURangeSLA {
// 	minData := math.Round(slices.MinFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	}).point.Cpu)
// 	maxData := math.Round(slices.MaxFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	}).point.Cpu)

// 	var cpuRangesSLA []CPURangeSLA

// 	// Add ranges from 0 to minData with default 100 SLA
// 	for i := 0.0; i < minData; i += 3 {
// 		end := i + 3
// 		if end > minData {
// 			end = minData
// 		}
// 		rangeID := int(math.Round(end))
// 		cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{
// 			RangeID: rangeID,
// 			SLA:     100.0,
// 		})
// 		fmt.Printf("Empty Range: %d - %d, SLA: 100.00%% (default)\n", int(i), rangeID)
// 	}

// 	// Add ranges with actual data
// 	for i := minData; i <= maxData; i += 3 {
// 		end := i + 3

// 		if end > maxData+1 {
// 			end = maxData + 1
// 		}

// 		chunk := make([]TimestampedSlaDataPoint, 0)
// 		for _, s := range data {
// 			if s.point.Cpu >= i && s.point.Cpu <= end {
// 				chunk = append(chunk, s)
// 			}
// 		}

// 		//fmt.Println("\n Chunk Data for demo")
// 		//printSlaData(chunk)
// 		if len(chunk) > 0 {
// 			cpuRange := calculateRangeSLA(chunk, 3.0)
// 			cpuRangesSLA = append(cpuRangesSLA, cpuRange)
// 			fmt.Printf("Chunk Range: %f - %f, SLA: %f /n", end-3, end, cpuRange.SLA)
// 		}

// 	}

// 	// Add ranges from maxData to 100 with default 0 SLA
// 	for i := maxData + 3; i <= 100; i += 3 {
// 		end := i + 3
// 		if end > 100 {
// 			end = 100
// 		}
// 		rangeID := int(math.Round(end))
// 		cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{
// 			RangeID: rangeID,
// 			SLA:     0.0,
// 		})
// 		fmt.Printf("Empty Range: %d - %d, SLA: 0.00%% (default)\n", int(i), rangeID)
// 	}

// 	return cpuRangesSLA
// }

// func returnSLARanges(data []TimestampedSlaDataPoint) []CPURangeSLA {
// 	if len(data) == 0 {
// 		return nil
// 	}

// 	totalEvents := float64(len(data))
// 	minEvents := int(math.Ceil(totalEvents / 100.0))
// 	if minEvents < 5 {
// 		minEvents = 5
// 	}

// 	cpuRangesSLA := []CPURangeSLA{
// 		{RangeID: 0, SLA: 100.0},
// 	}

// 	start := 1.0
// 	step := 1.0

// 	for start < 100.0 {
// 		intermediateStep := step
// 		end := math.Min(start+intermediateStep, 100.0)

// 		agg, events, ok := aggregateMetricsByCPURange(data, start, end)
// 		for !ok && intermediateStep < 3*step {
// 			intermediateStep += step
// 			end = math.Min(start+intermediateStep, 100.0)
// 			agg, events, ok = aggregateMetricsByCPURange(data, start, end)
// 		}

// 		if ok {
// 			for events < minEvents && intermediateStep < 3*step {
// 				intermediateStep += step
// 				end = math.Min(start+intermediateStep, 100.0)
// 				agg, events, ok = aggregateMetricsByCPURange(data, start, end)
// 				if !ok {
// 					break
// 				}
// 			}
// 		}

// 		if ok {
// 			agg.RangeID = int(math.Round(end))
// 			cpuRangesSLA = append(cpuRangesSLA, agg)
// 		}

// 		start = end
// 	}

// 	if len(cpuRangesSLA) == 0 || cpuRangesSLA[len(cpuRangesSLA)-1].RangeID != 100 {
// 		cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{
// 			RangeID: 100,
// 			SLA:     0.0,
// 		})
// 	}

// 	return cpuRangesSLA
// }

// func aggregateMetricsByCPURange(data []TimestampedSlaDataPoint, start, end float64) (CPURangeSLA, int, bool) {
// 	chunk := make([]TimestampedSlaDataPoint, 0)
// 	for _, s := range data {
// 		if s.point.Cpu >= start && s.point.Cpu <= end {
// 			chunk = append(chunk, s)
// 		}
// 	}
// 	if len(chunk) == 0 {
// 		return CPURangeSLA{}, 0, false
// 	}

// 	agg := calculateRangeSLA(chunk, 3.0)
// 	agg.RangeID = int(math.Round(end))
// 	return agg, len(chunk), true
// }

// // smoothValuesByCMA applies Simple Moving Average smoothing to metrics
// func smoothValuesByCMA(metrics []CPURangeSLA) []CPURangeSLA {
// 	if len(metrics) <= 6 {
// 		return metrics
// 	}

// 	smoothedMetrics := make([]CPURangeSLA, len(metrics))
// 	copy(smoothedMetrics, metrics)

// 	length := len(metrics)
// 	lags := int(math.Min(10, float64(length)/5))
// 	if lags < 1 {
// 		lags = 1
// 	}
// 	if lags > length {
// 		lags = length - 1
// 	}

// 	var sum float64
// 	sla_n_1 := 100.0

// 	for i := 1; i <= lags && i < length; i++ {
// 		slar := smoothedMetrics[i].SLA
// 		if slar > sla_n_1 {
// 			z := i
// 			for z > 0 && smoothedMetrics[z].SLA > smoothedMetrics[z-1].SLA {
// 				z--
// 			}
// 			for z < i {
// 				sum = smoothedMetrics[z-1].SLA*float64(z-1) + slar
// 				smoothedMetrics[z].SLA = sum / float64(z)
// 				z++
// 			}
// 		}
// 		sum += slar
// 		avg := sum / float64(i)
// 		smoothedMetrics[i].SLA = avg
// 		sla_n_1 = slar
// 	}

// 	a := length - 2
// 	if len(metrics) > lags {
// 		a = length - lags - 1
// 	}

// 	for i := lags; i < a && i < length; i++ {
// 		slar := smoothedMetrics[i].SLA
// 		if slar > sla_n_1 {
// 			z := i
// 			for z > 0 && smoothedMetrics[z].SLA > smoothedMetrics[z-1].SLA {
// 				z--
// 			}
// 			for z < i {
// 				slar1 := smoothedMetrics[i-lags].SLA
// 				sum = smoothedMetrics[z-1].SLA*float64(lags) + slar - slar1
// 				smoothedMetrics[z].SLA = sum / float64(lags)
// 				z++
// 			}
// 		}
// 		slar1 := smoothedMetrics[i-lags].SLA
// 		sum = sum + slar - slar1
// 		avg := sum / float64(lags)
// 		smoothedMetrics[i].SLA = avg
// 		sla_n_1 = slar
// 	}

// 	return ceilSLA(smoothedMetrics)
// }

// // // smoothValuesByCMA applies Simple Moving Average smoothing to metrics
// // func smoothValuesByCMA(metrics []CPURangeSLA) []CPURangeSLA {
// // 	if len(metrics) <= 6 {
// // 		return metrics
// // 	}

// // 	smoothedMetrics := make([]CPURangeSLA, len(metrics))
// // 	copy(smoothedMetrics, metrics)

// // 	length := len(metrics)
// // 	lags := int(math.Min(10, float64(length)/5))
// // 	if lags < 1 {
// // 		lags = 1
// // 	}
// // 	if lags > length {
// // 		lags = length - 1
// // 	}

// // 	var sum float64
// // 	sla_n_1 := 100.0

// // 	for i := 1; i <= lags && i < length; i++ {
// // 		slar := smoothedMetrics[i].SLA
// // 		if slar > sla_n_1 {
// // 			z := i
// // 			for z > 0 && metrics[z].SLA > metrics[z-1].SLA {
// // 				z--
// // 			}
// // 			for z < i {
// // 				sum = metrics[z-1].SLA*float64(z-1) + slar
// // 				metrics[z].SLA = sum / float64(z)
// // 				z++
// // 			}
// // 		}
// // 		sum += slar
// // 		avg := sum / float64(i)
// // 		metrics[i].SLA = avg
// // 		sla_n_1 = slar
// // 	}

// // 	a := length - 2
// // 	if len(metrics) > lags {
// // 		a = length - lags - 1
// // 	}

// // 	for i := lags; i < a && i < length; i++ {
// // 		slar := smoothedMetrics[i].SLA
// // 		if slar > sla_n_1 {
// // 			z := i
// // 			for z > 0 && metrics[z].SLA > metrics[z-1].SLA {
// // 				z--
// // 			}
// // 			for z < i {
// // 				slar1 := smoothedMetrics[i-lags].SLA
// // 				sum = metrics[z-1].SLA*float64(lags) + slar - slar1
// // 				metrics[z].SLA = sum / float64(lags)
// // 				z++
// // 			}
// // 		}
// // 		slar1 := smoothedMetrics[i-lags].SLA
// // 		sum = sum + slar - slar1
// // 		avg := sum / float64(lags)
// // 		metrics[i].SLA = avg
// // 		sla_n_1 = slar
// // 	}

// // 	return ceilSLA(metrics)
// // }

// // ceilSLA applies ceiling operation to SLA values
//
//	func ceilSLA(metrics []CPURangeSLA) []CPURangeSLA {
//		length := len(metrics)
//		for i := 1; i < length-1 && i < length; i++ {
//			if metrics[i].SLA > 100.0 {
//				metrics[i].SLA = 100.0
//			}
//			metrics[i].SLA = math.Ceil(metrics[i].SLA)
//		}
//		return metrics
//	}
// func returnSLARanges(data []TimestampedSlaDataPoint) []CPURangeSLA {
// 	if len(data) == 0 {
// 		return nil
// 	}

// 	totalEvents := float64(len(data))
// 	minEvents := int(math.Ceil(totalEvents / 100.0))
// 	if minEvents < 5 {
// 		minEvents = 5
// 	}

// 	cpuRangesSLA := []CPURangeSLA{
// 		{RangeID: 0, SLA: 100.0},
// 	}

// 	start := 1.0
// 	step := 1.0

// 	for start < 100.0 {
// 		intermediateStep := step
// 		end := math.Min(start+intermediateStep, 100.0)

// 		agg, events, ok := aggregateMetricsByCPURange(data, start, end)
// 		for !ok && intermediateStep < 3*step {
// 			intermediateStep += step
// 			end = math.Min(start+intermediateStep, 100.0)
// 			agg, events, ok = aggregateMetricsByCPURange(data, start, end)
// 		}

// 		if ok {
// 			for events < minEvents && intermediateStep < 3*step {
// 				intermediateStep += step
// 				end = math.Min(start+intermediateStep, 100.0)
// 				agg, events, ok = aggregateMetricsByCPURange(data, start, end)
// 				if !ok {
// 					break
// 				}
// 			}
// 		}

// 		if ok {
// 			agg.RangeID = int(math.Round(end))
// 			cpuRangesSLA = append(cpuRangesSLA, agg)
// 		}

// 		start = end
// 	}

// 	if len(cpuRangesSLA) == 0 || cpuRangesSLA[len(cpuRangesSLA)-1].RangeID != 100 {
// 		cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{
// 			RangeID: 100,
// 			SLA:     0.0,
// 		})
// 	}

// 	return cpuRangesSLA
// }

// func aggregateMetricsByCPURange(data []TimestampedSlaDataPoint, start, end float64) (CPURangeSLA, int, bool) {
// 	chunk := make([]TimestampedSlaDataPoint, 0)
// 	for _, s := range data {
// 		if s.point.Cpu >= start && s.point.Cpu <= end {
// 			chunk = append(chunk, s)
// 		}
// 	}
// 	if len(chunk) == 0 {
// 		return CPURangeSLA{}, 0, false
// 	}

// 	agg := calculateRangeSLA(chunk, 3.0)
// 	agg.RangeID = int(math.Round(end))
// 	return agg, len(chunk), true
// }

// func calculateRangeSLA(data []TimestampedSlaDataPoint, SlaRT float64) CPURangeSLA {
// 	rangeID := slices.MaxFunc(data, func(a, b TimestampedSlaDataPoint) int {
// 		return cmp.Compare(a.point.Cpu, b.point.Cpu)
// 	})
// 	maxID := int(math.Round(rangeID.point.Cpu))
// 	dev := len(data)
// 	div := slices.DeleteFunc(data, func(data TimestampedSlaDataPoint) bool {
// 		return data.point.RT >= SlaRT
// 	})

// 	rangeSLA := (float64(len(div)) / float64(dev)) * 100 / 100.0

// 	if float64(dev) > 5 {
// 		fmt.Printf("Range: %d, SLA: %.2f%% dev: %d , div: %d \n", maxID, rangeSLA*100, dev, len(div))
// 	}
// 	return CPURangeSLA{
// 		RangeID: maxID,
// 		SLA:     rangeSLA * 100.0,
// 	}
// }

// func smoothValuesByCMA(metrics []CPURangeSLA) []CPURangeSLA {
// 	if len(metrics) <= 6 {
// 		return metrics
// 	}

// 	smoothedMetrics := make([]CPURangeSLA, len(metrics))
// 	copy(smoothedMetrics, metrics)

// 	length := len(metrics)
// 	lags := int(math.Min(10, float64(length)/5))
// 	if lags < 1 {
// 		lags = 1
// 	}
// 	if lags > length {
// 		lags = length - 1
// 	}

// 	var sum float64
// 	sla_n_1 := 100.0

// 	for i := 1; i <= lags && i < length; i++ {
// 		slar := smoothedMetrics[i].SLA
// 		if slar > sla_n_1 {
// 			z := i
// 			for z > 0 && smoothedMetrics[z].SLA > smoothedMetrics[z-1].SLA {
// 				z--
// 			}
// 			for z < i {
// 				sum = smoothedMetrics[z-1].SLA*float64(z-1) + slar
// 				smoothedMetrics[z].SLA = sum / float64(z)
// 				z++
// 			}
// 		}
// 		sum += slar
// 		avg := sum / float64(i)
// 		smoothedMetrics[i].SLA = avg
// 		sla_n_1 = slar
// 	}

// 	a := length - 2
// 	if len(metrics) > lags {
// 		a = length - lags - 1
// 	}

// 	for i := lags; i < a && i < length; i++ {
// 		slar := smoothedMetrics[i].SLA
// 		if slar > sla_n_1 {
// 			z := i
// 			for z > 0 && smoothedMetrics[z].SLA > smoothedMetrics[z-1].SLA {
// 				z--
// 			}
// 			for z < i {
// 				slar1 := smoothedMetrics[i-lags].SLA
// 				sum = smoothedMetrics[z-1].SLA*float64(lags) + slar - slar1
// 				smoothedMetrics[z].SLA = sum / float64(lags)
// 				z++
// 			}
// 		}
// 		slar1 := smoothedMetrics[i-lags].SLA
// 		sum = sum + slar - slar1
// 		avg := sum / float64(lags)
// 		smoothedMetrics[i].SLA = avg
// 		sla_n_1 = slar
// 	}

// 	return ceilSLA(smoothedMetrics)
// }

// func smoothValuesBySMA(metrics []CPURangeSLA) []CPURangeSLA {
//     length := len(metrics)
//     if length <= 6 {
//         return metrics
//     }

//     // Window size (lags)
//     window := int(math.Min(10, float64(length)))

//     smoothed := make([]CPURangeSLA, length)
//     copy(smoothed, metrics)

//     for i := 0; i < length; i++ {
//         sum := 0.0
//         count := 0

//         // Look back 'window' steps
//         for j := i; j >= 0 && j > i-window; j-- {
//             sum += metrics[j].SLA
//             count++
//         }

//         smoothed[i].SLA = sum / float64(count)
//     }

//     return ceilSLA(smoothed)
// }

// func ceilSLA(metrics []CPURangeSLA) []CPURangeSLA {
// 	length := len(metrics)
// 	for i := 1; i < length-1 && i < length; i++ {
// 		if metrics[i].SLA > 100.0 {
// 			metrics[i].SLA = 100.0
// 		}
// 		metrics[i].SLA = math.Ceil(metrics[i].SLA)
// 	}
// 	return metrics
// }

func returnSLARanges(data []TimestampedSlaDataPoint) []CPURangeSLA {
	if len(data) == 0 {
		fmt.Println("[DEBUG] No data provided, returning default 100->0 range")
		return []CPURangeSLA{{RangeID: 0, SLA: 100}, {RangeID: 100, SLA: 0}}
	}

	totalEvents := float64(len(data))
	minEvents := int(math.Ceil(totalEvents / 100.0))
	if minEvents < 5 {
		minEvents = 5
	}
	fmt.Printf("[DEBUG] Total Events: %.0f, MinEvents Threshold: %d\n", totalEvents, minEvents)
	cpuRangesSLA := []CPURangeSLA{}
	currentStart := 0.0

	// 2. Generate Ranges (Width: 1% to 3%)
	for currentStart < 100.0 {
		var bestMatch CPURangeSLA
		foundData := false
		actualWidthUsed := 1.0

		// Try expanding width from 1% up to 3% to find enough events
		for width := 1.0; width <= 3.0; width += 1.0 {
			currentEnd := math.Min(currentStart+width, 100.0)

			// aggregateMetricsByCPURange returns SLA, EventCount, and Success
			agg, events, ok := aggregateMetricsByCPURange(data, currentStart, currentEnd)

			actualWidthUsed = width
			if ok && events >= minEvents {
				bestMatch = agg
				bestMatch.RangeID = int(math.Round(currentEnd))
				foundData = true
				break // Found sufficient data, stop expanding this range
			}

			if currentEnd >= 100.0 {
				break
			}
		}

		if foundData {
			cpuRangesSLA = append(cpuRangesSLA, bestMatch)
		} else {
			// No data found after 3% expansion, mark as gap for interpolation (-1.0)
			gapEnd := math.Min(currentStart+3.0, 100.0)
			cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{
				RangeID: int(math.Round(gapEnd)),
				SLA:     -1.0,
			})
			actualWidthUsed = 3.0
		}
		currentStart += actualWidthUsed
	}

	// 3. Final Guardrail: Force 100% CPU to 0% SLA
	lastIdx := len(cpuRangesSLA) - 1
	if cpuRangesSLA[lastIdx].RangeID >= 97 {
		cpuRangesSLA[lastIdx].RangeID = 100
		cpuRangesSLA[lastIdx].SLA = 0.0
	} else {
		cpuRangesSLA = append(cpuRangesSLA, CPURangeSLA{RangeID: 100, SLA: 0.0})
	}

	// 4. Pre-Data Hard-Lock (Keep 100% SLA until the first real data dip)
	// This prevents the {10 99} {13 99} issue
	firstRealDipIdx := -1
	for i, r := range cpuRangesSLA {
		if r.SLA >= 0 && r.SLA < 100.0 {
			firstRealDipIdx = i
			break
		}
	}
	if firstRealDipIdx > 0 {
		for i := 0; i < firstRealDipIdx; i++ {
			cpuRangesSLA[i].SLA = 100.0
		}
	}

	// 5. Interpolate Gaps (Fill the -1.0 values)
	interpolated := interpolateGaps(cpuRangesSLA)

	// 6. Smooth via SMA/CMA
	return smoothValuesBySMA(interpolated)
}

func interpolateGaps(metrics []CPURangeSLA) []CPURangeSLA {
	length := len(metrics)
	for i := 1; i < length-1; i++ {
		if metrics[i].SLA < 0 {
			nextIdx := -1
			for j := i + 1; j < length; j++ {
				if metrics[j].SLA >= 0 {
					nextIdx = j
					break
				}
			}

			if nextIdx != -1 {
				lastVal, nextVal := metrics[i-1].SLA, metrics[nextIdx].SLA
				dist := float64(nextIdx - (i - 1))
				slope := (nextVal - lastVal) / dist

				// fmt.Printf("[DEBUG] Interpolating gap between ID %d (SLA: %.1f) and ID %d (SLA: %.1f). Slope: %.2f\n",
				// 	metrics[i-1].RangeID, lastVal, metrics[nextIdx].RangeID, nextVal, slope)

				for k := i; k < nextIdx; k++ {
					metrics[k].SLA = metrics[k-1].SLA + slope
				}
				i = nextIdx - 1
			}
		}
	}
	return metrics
}

// func smoothValuesBySMA(metrics []CPURangeSLA) []CPURangeSLA {
// 	length := len(metrics)
// 	if length <= 1 {
// 		return metrics
// 	}

// 	// PRESERVED: Your exact Lags logic
// 	lags := int(math.Min(10, float64(length)/5))
// 	if lags < 1 {
// 		lags = 1
// 	}

// 	fmt.Printf("[DEBUG] Smoothing with SMA. Total Points: %d, Lags: %d\n", length, lags)
// 	smoothed := make([]CPURangeSLA, length)
// 	copy(smoothed, metrics)

// 	for i := 0; i < length; i++ {
// 		var sum float64
// 		count := 0

// 		// Use 'lags' as the sliding window
// 		for j := i; j >= 0 && j > i-lags; j-- {
// 			sum += metrics[j].SLA
// 			count++
// 		}

// 		smoothed[i].SLA = sum / float64(count)
// 	}

// 	return ceilSLA(smoothed)
// }

func smoothValuesBySMA(metrics []CPURangeSLA) []CPURangeSLA {
	length := len(metrics)
	if length <= 1 {
		return metrics
	}

	lags := int(math.Min(10, float64(length)/5))
	if lags < 1 {
		lags = 1
	}

	smoothed := make([]CPURangeSLA, length)
	copy(smoothed, metrics)
	fmt.Printf("[DEBUG] Smoothing with SMA. Total Points: %d, Lags: %d\n", length, lags)
	for i := 0; i < length; i++ {
		var sum float64
		count := 0
		for j := i; j >= 0 && j > i-lags; j-- {
			sum += metrics[j].SLA
			count++
		}
		smoothed[i].SLA = sum / float64(count)
	}

	// --- NEW GUARDRAIL START ---

	// 1. If the original data started at 100, ensure the smoothed start is 100
	if metrics[0].SLA == 100.0 {
		smoothed[0].SLA = 100.0
	}

	// 2. FORCE THE END TO ZERO
	// Since 100% CPU must be 0 SLA, we override the smoothing result here.
	// This stops the previous 14%, 15% values from "pulling up" the zero.
	smoothed[length-1].SLA = 0.0

	// --- NEW GUARDRAIL END ---

	return ceilSLA(smoothed)
}

func aggregateMetricsByCPURange(data []TimestampedSlaDataPoint, start, end float64) (CPURangeSLA, int, bool) {
	chunk := make([]TimestampedSlaDataPoint, 0)
	for _, s := range data {
		if s.point != nil && s.point.Cpu >= start && s.point.Cpu <= end {
			chunk = append(chunk, s)
		}

	}
	if len(chunk) < 5 {
		return CPURangeSLA{}, 0, false
	}

	// Internal calculation
	rangeID := slices.MaxFunc(chunk, func(a, b TimestampedSlaDataPoint) int {
		return cmp.Compare(a.point.Cpu, b.point.Cpu)
	})

	successCount := 0
	for _, d := range chunk {
		if d.point.RT < 3.0 {
			successCount++
		}
	}

	fmt.Printf("Range: %d, SLA: %.2f%% | Successes: %d, Total Events: %d\n",
		int(math.Round(rangeID.point.Cpu)),
		(float64(successCount)/float64(len(chunk)))*100.0,
		successCount,
		len(chunk))

	return CPURangeSLA{
		RangeID: int(math.Round(rangeID.point.Cpu)),
		SLA:     (float64(successCount) / float64(len(chunk))) * 100.0,
	}, len(chunk), true
}

func ceilSLA(metrics []CPURangeSLA) []CPURangeSLA {
	for i := range metrics {
		if metrics[i].SLA > 100.0 {
			metrics[i].SLA = 100.0
		}
		metrics[i].SLA = math.Ceil(metrics[i].SLA)
	}
	return metrics
}

type IQRStats struct {
	Lower float64
	Upper float64
}

// 1. Calculate IQR Bounds (Java calculateIQ equivalent)
func GetIQRBounds(values []float64) IQRStats {
	if len(values) == 0 {
		return IQRStats{}
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	n := len(sorted)
	q1 := sorted[n/4]
	q3 := sorted[3*n/4]
	iqr := q3 - q1

	return IQRStats{
		Lower: q1 - (1.5 * iqr),
		Upper: q3 + (1.5 * iqr),
	}
}

// 2. Filter and Return: Performs calculations and removes outliers in one pipeline
func FilterOutliers(data []TimestampedSlaDataPoint) []TimestampedSlaDataPoint {
	if len(data) < 10 {
		return data
	}

	// Slices for IQR analysis (RPS/Pod and Efficiency/RPS/CPU)
	podRpsVals := make([]float64, 0, len(data))
	cpuRpsVals := make([]float64, 0, len(data))

	// Temporary storage for pre-calculated values to avoid re-calculating during filter
	type helper struct {
		rpsPod float64
		rpsCPU float64
	}
	helpers := make([]helper, len(data))

	for i, ts := range data {
		p := ts.point
		if ts.point == nil || ts.point.Pods <= 0 || ts.point.Cpu <= 0 {
			continue
		}

		rpsPod := p.BTCount / p.Pods
		rpsCPU := rpsPod / p.Cpu

		podRpsVals = append(podRpsVals, rpsPod)
		cpuRpsVals = append(cpuRpsVals, rpsCPU)
		helpers[i] = helper{rpsPod: rpsPod, rpsCPU: rpsCPU}
	}

	// Get the statistical "fences"
	podStats := GetIQRBounds(podRpsVals)
	cpuStats := GetIQRBounds(cpuRpsVals)

	// Filter original slice based on calculated bounds
	filtered := make([]TimestampedSlaDataPoint, 0)
	for i, ts := range data {
		h := helpers[i]

		// Skip if calculation was invalid (div by zero check above)
		if h.rpsPod == 0 && h.rpsCPU == 0 {
			continue
		}

		// Keep if within both RPS/Pod and Efficiency (RPS/CPU) bounds
		if h.rpsPod >= podStats.Lower && h.rpsPod <= podStats.Upper &&
			h.rpsCPU >= cpuStats.Lower && h.rpsCPU <= cpuStats.Upper {

			// // Update the SLA field based on the threshold before returning
			// if ts.point.RT > RESPONSE_TIME_THRESHOLD {
			// 	ts.point.Sla = 0.0
			// } else {
			// 	ts.point.Sla = 100.0
			// }

			filtered = append(filtered, ts)
		}
	}

	return filtered
}

func appendSlaDataToCSV(filename string, newData []TimestampedSlaDataPoint, vpa int64) {
	// Check if file exists to see if we need headers

	_, err := os.Stat(filename)
	fileExisted := !os.IsNotExist(err)

	// Open for appending (O_APPEND), Create if missing (O_CREATE), Write-only (O_WRONLY)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("[ERROR] Permission Denied: Cannot create/open %s: %v\n", filename, err)
		return
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExisted {
		headers := []string{"Timestamp", "CPULimits", "CPU", "MaxCPU", "AvgCPU", "Memory", "SLA", "AvgRT", "RT", "BTCount", "Pods", "VPA"}
		writer.Write(headers)
	}

	var lastTimestamp time.Time

	// 2. Attempt to find the last entry (If file exists)
	fileBytes, err := os.ReadFile(filename)
	if err == nil {
		lines := strings.Split(string(fileBytes), "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if line == "" || strings.HasPrefix(line, "Timestamp") {
				continue
			}
			columns := strings.Split(line, ",")
			if len(columns) > 0 {
				if t, parseErr := time.Parse(time.RFC3339, columns[0]); parseErr == nil {
					lastTimestamp = t
					break
				}
			}
		}
	}

	count := 0
	for _, dp := range newData {
		if dp.point != nil && dp.timestamp.After(lastTimestamp) {
			row := []string{
				dp.timestamp.Format(time.RFC3339),
				strconv.FormatFloat(dp.point.CPULimits, 'f', 2, 64),
				strconv.FormatFloat(dp.point.Cpu, 'f', 2, 64),
				strconv.FormatFloat(dp.point.MaxCpu, 'f', 2, 64),
				strconv.FormatFloat(dp.point.AvgCpu, 'f', 2, 64),
				strconv.FormatFloat(dp.point.Memory, 'f', 2, 64),
				strconv.FormatFloat(dp.point.Sla, 'f', 2, 64),
				strconv.FormatFloat(dp.point.AvgRT, 'f', 4, 64),
				strconv.FormatFloat(dp.point.RT, 'f', 4, 64),
				strconv.FormatFloat(dp.point.BTCount, 'f', 0, 64),
				strconv.FormatFloat(dp.point.Pods, 'f', 0, 64),
				strconv.FormatInt(vpa, 10),
			}
			fmt.Printf("[debug] %w", row)
			writer.Write(row)
			count++
		}
	}

	if count > 0 {
		fmt.Printf("[INFO] Appended %d new records to %s\n", count, filename)
	}

}

func appendSlaDataToCSV2(aggregatedData []TimestampedSlaDataPoint, csvFilename string) {
	// 1. Ensure we are using a writable directory in the container
	if !strings.HasPrefix(csvFilename, "/tmp/") {
		csvFilename = "/tmp/sla_results.csv"
	}

	var lastTimestamp time.Time

	// 2. Attempt to find the last entry (If file exists)
	fileBytes, err := os.ReadFile(csvFilename)
	if err == nil {
		lines := strings.Split(string(fileBytes), "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if line == "" || strings.HasPrefix(line, "Timestamp") {
				continue
			}
			columns := strings.Split(line, ",")
			if len(columns) > 0 {
				if t, parseErr := time.Parse(time.RFC3339, columns[0]); parseErr == nil {
					lastTimestamp = t
					break
				}
			}
		}
	}

	// 3. CRITICAL: Open with O_CREATE and O_APPEND
	// If you don't use O_CREATE, the file will never be born!
	file, err := os.OpenFile(csvFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("[ERROR] Permission Denied: Cannot create/open %s: %v\n", csvFilename, err)
		return
	}
	defer file.Close()

	// 4. Check if we need to write the header
	info, _ := file.Stat()
	if info.Size() == 0 {
		file.WriteString("Timestamp,CPULimit\n")
	}

	// 5. Append only NEW, NON-NIL data
	count := 0
	for _, d := range aggregatedData {
		// Safety check for the SIGSEGV we saw earlier
		if d.point != nil && d.timestamp.After(lastTimestamp) {
			line := fmt.Sprintf("%s,%.2f\n", d.timestamp.Format(time.RFC3339), d.point.CPULimits)
			file.WriteString(line)
			count++
		}
	}

	if count > 0 {
		fmt.Printf("[INFO] Appended %d new records to %s\n", count, csvFilename)
	}
}
