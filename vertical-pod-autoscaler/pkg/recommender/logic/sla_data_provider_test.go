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
	"errors"
	"fmt"
	"testing"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockPrometheusAPI struct {
	mock.Mock
	prometheusv1.API
}

func (m *mockPrometheusAPI) QueryRange(ctx context.Context, query string, r prometheusv1.Range, opts ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error) {
	args := m.Called(ctx, query, r)
	var returnArg prommodel.Value
	if args.Get(0) != nil {
		returnArg = args.Get(0).(prommodel.Value)
	}
	return returnArg, nil, args.Error(1)
}

func TestGetSlaNoDataSuccess(t *testing.T) {
	mockClient := &mockPrometheusAPI{}
	provider := &dataProvider{
		prometheusClient: mockClient,
		queryTimeout:     10 * time.Second,
	}

	expectedCpuQuery := fmt.Sprintf(queries["CpuQuery"], "demo1")
	expectedSlaQuery := fmt.Sprintf(queries["SlaQuery"], "demo1")

	mockClient.On("QueryRange", mock.Anything, expectedCpuQuery, mock.AnythingOfType("v1.Range")).Return(
		prommodel.Matrix{}, nil).Once()
	mockClient.On("QueryRange", mock.Anything, expectedSlaQuery, mock.AnythingOfType("v1.Range")).Return(
		prommodel.Matrix{}, nil).Once()

	result, err := provider.GetSlaData("demo1", time.Hour, time.Minute)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestGetSlaDataSingleSeriesSuccess(t *testing.T) {
	mockClient := &mockPrometheusAPI{}
	provider := &dataProvider{
		prometheusClient: mockClient,
		queryTimeout:     10 * time.Second,
	}

	ts := prommodel.TimeFromUnix(1000)
	singleSeries := prommodel.Matrix{
		&prommodel.SampleStream{
			Metric: prommodel.Metric{"container": "demo1"},
			Values: []prommodel.SamplePair{
				{Timestamp: ts, Value: 42.0},
			},
		},
	}

	expectedCpuQuery := fmt.Sprintf(queries["CpuQuery"], "demo1")
	expectedSlaQuery := fmt.Sprintf(queries["SlaQuery"], "demo1")

	mockClient.On("QueryRange", mock.Anything, expectedCpuQuery, mock.AnythingOfType("v1.Range")).Return(
		singleSeries, nil).Once()
	mockClient.On("QueryRange", mock.Anything, expectedSlaQuery, mock.AnythingOfType("v1.Range")).Return(
		singleSeries, nil).Once()

	result, err := provider.GetSlaData("demo1", time.Hour, time.Minute)
	assert.Nil(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, ts.Time(), result[0].timestamp)
	mockClient.AssertExpectations(t)
}

func TestGetSlaDataPrometheusError(t *testing.T) {
	mockClient := &mockPrometheusAPI{}
	provider := &dataProvider{
		prometheusClient: mockClient,
		queryTimeout:     10 * time.Second,
	}

	mockClient.On("QueryRange", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("v1.Range")).Return(
		nil, errors.New("prometheus error"))

	_, err := provider.GetSlaData("demo1", time.Hour, time.Minute)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "prometheus error")
}

func TestGetSlaDataMultipleSeriesError(t *testing.T) {
	mockClient := &mockPrometheusAPI{}
	provider := &dataProvider{
		prometheusClient: mockClient,
		queryTimeout:     10 * time.Second,
	}

	multiSeries := prommodel.Matrix{
		&prommodel.SampleStream{Metric: prommodel.Metric{"container": "demo1", "pod": "demo1-abc"}},
		&prommodel.SampleStream{Metric: prommodel.Metric{"container": "demo1", "pod": "demo1-xyz"}},
	}

	mockClient.On("QueryRange", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("v1.Range")).Return(
		multiSeries, nil)

	_, err := provider.GetSlaData("demo1", time.Hour, time.Minute)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "expecting single series")
}

func TestCreateSlaDataProvider(t *testing.T) {
	mockClient := &mockPrometheusAPI{}
	provider := CreateSlaDataProvider(mockClient, 10*time.Second)
	assert.NotNil(t, provider)
}
