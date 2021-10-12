/*
Copyright 2021 The KEDA Authors

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

package scalers

import (
	"net/http"
	"testing"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
)

const (
	clusterName = "sample_cluster"
	region      = "eastus"
)

type parseDataExplorerMetadataTestData struct {
	metadata map[string]string
	isError  bool
}

type DataExplorerMetricIdentifier struct {
	metadataTestData *parseDataExplorerMetadataTestData
	name             string
}

//var (
//	query = "let x = 10; let y = 1; print MetricValue = x, Threshold = y;"
//)

// Faked parameters
var sampleDataExplorerResolvedEnv = map[string]string{
	tenantID:     "d248da64-0e1e-4f79-b8c6-72ab7aa055eb",
	clientID:     "41826dd4-9e0a-4357-a5bd-a88ad771ea7d",
	clientSecret: "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs",
	clusterName:  "sample_cluster",
	region:       "eastus",
}

// A complete valid authParams with username and passwd (Faked)
var DataExplorerAuthParams = map[string]string{
	"tenantId":     "d248da64-0e1e-4f79-b8c6-72ab7aa055eb",
	"clientId":     "41826dd4-9e0a-4357-a5bd-a88ad771ea7d",
	"clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs",
	"clusterName":  "sample_cluster",
	"region":       "eastus",
}

// An invalid authParams without username and passwd
var emptyDataExplorerAuthParams = map[string]string{
	"tenantId":     "",
	"clientId":     "",
	"clientSecret": "",
	"clusterName":  "",
	"region":       "",
}

var testDataExplorerMetadata = []parseDataExplorerMetadataTestData{
	// nothing passed
	{map[string]string{}, true},
	// Missing tenantId should fail
	{map[string]string{"tenantId": "", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, true},
	// Missing clientId, should fail
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, true},
	// Missing clientSecret, should fail
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, true},
	// Missing clusterName, should fail
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "", "region": "eastus", "query": query, "threshold": "1900000000"}, true},
	// Missing query, should fail
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": "", "threshold": "1900000000"}, true},
	// Missing threshold, should fail
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": ""}, true},
	// All parameters set, should succeed
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, false},
	// All parameters set, should succeed
	{map[string]string{"tenantIdFromEnv": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientIdFromEnv": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecretFromEnv": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "workspaceIdFromEnv": "074dd9f8-c368-4220-9400-acb6e80fc325", "query": query, "threshold": "1900000000"}, false}, //todo almog - clustername from env?
}

var DataExplorerMetricIdentifiers = []DataExplorerMetricIdentifier{
	{&testDataExplorerMetadata[7], "azure-data-explorer-sample_cluster-eastus"},
}

var testDataExplorerMetadataWithEmptyAuthParams = []parseDataExplorerMetadataTestData{
	// nothing passed
	{map[string]string{}, true},
	// Missing query, should fail
	{map[string]string{"query": "", "threshold": "1900000000"}, true},
	// Missing threshold, should fail
	{map[string]string{"query": query, "threshold": ""}, true},
	// All parameters set, should succeed
	{map[string]string{"query": query, "threshold": "1900000000"}, true},
}

var testDataExplorerMetadataWithAuthParams = []parseDataExplorerMetadataTestData{
	{map[string]string{"tenantId": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientId": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clientSecret": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, false},
}

var testDataExplorerMetadataWithPodIdentity = []parseDataExplorerMetadataTestData{
	{map[string]string{"clusterName": "sample_cluster", "region": "eastus", "query": query, "threshold": "1900000000"}, false},
}

func TestDataExplorerParseMetadata(t *testing.T) {
	for _, testData := range testDataExplorerMetadata {
		_, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadata, AuthParams: nil, PodIdentity: ""})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}

	// test with missing auth params should all fail
	for _, testData := range testDataExplorerMetadataWithEmptyAuthParams {
		_, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadata, AuthParams: emptyDataExplorerAuthParams, PodIdentity: ""})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}

	// test with complete auth params should not fail
	for _, testData := range testDataExplorerMetadataWithAuthParams {
		_, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadata, AuthParams: DataExplorerAuthParams, PodIdentity: ""})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}

	// test with podIdentity params should not fail
	for _, testData := range testDataExplorerMetadataWithPodIdentity {
		_, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadata, AuthParams: DataExplorerAuthParams, PodIdentity: kedav1alpha1.PodIdentityProviderAzure})
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestDataExplorerGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range DataExplorerMetricIdentifiers {
		meta, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadataTestData.metadata, AuthParams: nil, PodIdentity: ""})
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		cache := &sessionCache{metricValue: 1, metricThreshold: 2}
		mockDataExplorerScaler := azureDataExplorerScaler{
			metadata:   meta,
			cache:      cache,
			name:       "test-so",
			namespace:  "test-ns",
			httpClient: http.DefaultClient,
		}

		metricSpec := mockDataExplorerScaler.GetMetricSpecForScaling()
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

var testParseMetadataMetricNameKusto = []parseMetadataMetricNameTestData{ //todo almog - fix
	// clusterName
	{map[string]string{"tenantIdFromEnv": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientIdFromEnv": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clusterName": "sample_cluster", "region": "eastus", "clientSecretFromEnv": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "workspaceIdFromEnv": "074dd9f8-c368-4220-9400-acb6e80fc325", "query": query, "threshold": "1900000000"}, "azure-data-explorer-sample_cluster-eastus"},
	// Custom Name
	{map[string]string{"metricName": "testName", "tenantIdFromEnv": "d248da64-0e1e-4f79-b8c6-72ab7aa055eb", "clientIdFromEnv": "41826dd4-9e0a-4357-a5bd-a88ad771ea7d", "clusterName": "sample_cluster", "region": "eastus", "clientSecretFromEnv": "U6DtAX5r6RPZxd~l12Ri3X8J9urt5Q-xs", "workspaceIdFromEnv": "074dd9f8-c368-4220-9400-acb6e80fc325", "query": query, "threshold": "1900000000"}, "azure-data-explorer-testName-testName"}, //todo almog - fix
}

func TestDataExplorerParseMetadataMetricName(t *testing.T) {
	for _, testData := range testParseMetadataMetricNameKusto {
		meta, err := parseAzureDataExplorerMetadata(&ScalerConfig{ResolvedEnv: sampleDataExplorerResolvedEnv, TriggerMetadata: testData.metadata, AuthParams: nil, PodIdentity: ""})
		if err != nil {
			t.Error("Expected success but got error", err)
		}
		if meta.metricName != testData.metricName {
			t.Errorf("Expected %s but got %s", testData.metricName, meta.metricName)
		}
	}
}
