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
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type SlaDataPoint struct {
}

type SlaDataProvider interface {
	GetSlaData(containerName string) ([]SlaDataPoint, error)
}

type dataProvider struct {
	prometheusClient prometheusv1.API
	queryTimeout     time.Duration
}

func (r *dataProvider) GetSlaData(containerName string) ([]SlaDataPoint, error) {
	//this is wrong in so many ways, but it's a good start
	ctx, cancel := context.WithTimeout(context.Background(), r.queryTimeout)
	defer cancel()

	rangeQuery := prometheusv1.Range{
		Start: time.Now().Add(-time.Hour),
		End:   time.Now(),
		Step:  time.Minute,
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
		query := fmt.Sprintf(queryTemplate, containerName)
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
					Cpu: math.NaN(),
					Sla: math.NaN(),
				}
			}
			reflect.ValueOf(&point).Elem().FieldByName(field.Name).SetFloat(float64(value.Value))
			byTimestamp[timestamp] = point
		}
	}
	println(data)
	return make([]SlaDataPoint, 0), nil
}

func CreateSlaDataProvider(
	prometheusClient prometheusv1.API,
	queryTimeout time.Duration) SlaDataProvider {
	return &dataProvider{
		prometheusClient: prometheusClient,
		queryTimeout:     queryTimeout,
	}
}
