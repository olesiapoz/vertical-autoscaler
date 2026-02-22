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
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

type slaPodResourceRecommender struct {
	slaDataProvider               SlaDataProvider
	vanillaPodResourceRecommender PodResourceRecommender
}

func (r *slaPodResourceRecommender) GetRecommendedPodResources(containerNameToAggregateStateMap model.ContainerNameToAggregateStateMap) RecommendedPodResources {
	for containerName := range containerNameToAggregateStateMap {
		println(r.slaDataProvider.GetSlaData(containerName))
	}
	return r.vanillaPodResourceRecommender.GetRecommendedPodResources(containerNameToAggregateStateMap)
}

func CreateSlaPodResourceRecommender(
	slaDataProvider SlaDataProvider,
	vanillaPodResourceRecommender PodResourceRecommender) PodResourceRecommender {

	return &slaPodResourceRecommender{
		slaDataProvider:               slaDataProvider,
		vanillaPodResourceRecommender: vanillaPodResourceRecommender,
	}
}
