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
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
)

var queries = map[string]string{
	// only use %s for label values; groupings are fixed
	"CpuQuery":     `avg(rate(container_cpu_usage_seconds_total{container=%s}[1m]) * on (pod) group_left kube_pod_container_status_ready{container=%s} > 0)`,
	"SlaQuery":     `(sum by (container, pod, namespace) (rate(request_duration_bucket{le="3.0", container=%s, job="demo1-service"}[15m])) / sum by (container, pod, namespace) (rate(request_duration_count{container=%s, job="demo1-service"}[15m]))) * 100`,
	"AvgRTQuery":   `(sum by (container, pod, namespace) (rate(request_duration_sum{container=%s, job="demo1-service"}[1m])) / sum by (container, pod, namespace) (rate(request_duration_count{container=%s, job="demo1-service"}[1m])))`,
	"BTCountQuery": `sum by (container) (rate(bt_count_total{container=%s, job="demo1-service"}[1m]))`,
	// count ready containers by container label only
	"PodContainerCountQuery": `count(kube_pod_container_status_ready{container=%s, condition="true"})`,
}

type TimestampedSlaDataPoint struct {
	timestamp time.Time
	point     *SlaDataPoint
}

type SlaDataPoint struct {
	Cpu     float64 `query:"CpuQuery"`
	Sla     float64 `query:"SlaQuery"`
	AvgRT   float64 `query:"AvgRTQuery"`
	BTCount float64 `query:"BTCountQuery"`
	Pods    float64 `query:"PodContainerCountQuery"`
}

type SlaDataProvider interface {
	GetSlaData(containerName string, durationFromNow time.Duration, step time.Duration) ([]TimestampedSlaDataPoint, error)
}

type dataProvider struct {
	prometheusClient prometheusv1.API
	queryTimeout     time.Duration
}

func (r *dataProvider) GetSlaData(containerName string, durationFromNow time.Duration, step time.Duration) ([]TimestampedSlaDataPoint, error) {
	//this is wrong in so many ways, but it's a good start
	ctx, cancel := context.WithTimeout(context.Background(), r.queryTimeout)
	defer cancel()

	rangeQuery := prometheusv1.Range{
		Start: time.Now().Add(-durationFromNow),
		End:   time.Now(),
		Step:  step,
	}

	var byTimestamp = make(map[time.Time]SlaDataPoint, 0)

	t := reflect.TypeOf(SlaDataPoint{})
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		queryName := field.Tag.Get("query")
		if queryName == "" {
			continue
		}
		queryTemplate, ok := queries[queryName]
		if !ok {
			return nil, fmt.Errorf("unknown query constant: %s", queryName)
		}

		// build args matching number of %s placeholders and always pass quoted container name
		placeholderCount := strings.Count(queryTemplate, "%s")
		args := make([]interface{}, placeholderCount)
		quoted := strconv.Quote(containerName)
		for j := 0; j < placeholderCount; j++ {
			args[j] = quoted
		}
		query := fmt.Sprintf(queryTemplate, args...)

		fmt.Printf("\nExecuting query for %s of container: %s\n", field.Name, containerName)
		fmt.Printf("Executing query for %s: %s\n", field.Name, query)

		data, _, err := r.prometheusClient.QueryRange(ctx, query, rangeQuery)
		if err != nil {
			return nil, err
		}
		matrix, ok := data.(prommodel.Matrix)
		if !ok {
			return nil, fmt.Errorf("unexpected data type: %T", data)
		}
		if len(matrix) == 0 {
			// no data - no problem
			continue
		}
		if len(matrix) != 1 {
			return nil, fmt.Errorf("expecting single series: %s", query)
		}
		// iterate sample and merge data points
		for _, value := range matrix[0].Values {
			timestamp := value.Timestamp.Time()
			point, exists := byTimestamp[timestamp]
			if !exists {
				point = SlaDataPoint{
					Cpu:     math.NaN(),
					Sla:     math.NaN(),
					AvgRT:   math.NaN(),
					BTCount: math.NaN(),
					Pods:    math.NaN(),
				}
			}
			reflect.ValueOf(&point).Elem().FieldByName(field.Name).SetFloat(float64(value.Value))
			byTimestamp[timestamp] = point
		}
	}
	// put and order by timestamp
	result := make([]TimestampedSlaDataPoint, 0)
	for timestamp, point := range byTimestamp {
		result = append(result, TimestampedSlaDataPoint{
			timestamp: timestamp,
			point:     &point,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].timestamp.Before(result[j].timestamp)
	})

	return result, nil
}

func CreateSlaDataProvider(
	prometheusClient prometheusv1.API,
	queryTimeout time.Duration) SlaDataProvider {
	return &dataProvider{
		prometheusClient: prometheusClient,
		queryTimeout:     queryTimeout,
	}
}
