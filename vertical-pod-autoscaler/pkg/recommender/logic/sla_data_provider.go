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

	//query := fmt.Sprintf("container_cpu_usage_seconds_total{container=\"%s\"}", containerName)
	slaQuery := fmt.Sprintf(`(sum by (container, pod, namespace) (rate(request_duration_bucket{le="3.0", container="%s", job="demo1-service"}[%dm])) / sum by (container, pod, namespace) (rate(request_duration_count{container="%s", job="demo1-service"}[%dm]))) * 100`,
		containerName, int(rangeQuery.Step.Minutes()), containerName, int(rangeQuery.Step.Minutes()))
	data, _, err := r.prometheusClient.QueryRange(ctx, slaQuery, rangeQuery)
	if err != nil {
		return nil, err
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
