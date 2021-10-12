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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	v2beta2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	deMiEndpoint    = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=" + deQueryEndpoint
	deQueryEndpoint = "https://%s.%s.kusto.windows.net"
)

type azureDataExplorerScaler struct {
	metadata   *azureDataExplorerMetadata
	cache      *sessionCache
	name       string
	namespace  string
	httpClient *http.Client
}

type azureDataExplorerMetadata struct {
	tenantID     string
	clientID     string
	clientSecret string
	clusterName  string
	podIdentity  string
	query        string
	region       string
	threshold    int64
	metricName   string // Custom metric name for trigger
}

var DataExplorerLog = logf.Log.WithName("azure_data_explorer_scaler")

// NewAzureDataExplorerScaler creates a new Azure Data Explorer Scaler
func NewAzureDataExplorerScaler(config *ScalerConfig) (Scaler, error) {
	azureDataExplorerMetadata, err := parseAzureDataExplorerMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Data Explorer scaler. Scaled object: %s. Namespace: %s. Inner Error: %v", config.Name, config.Namespace, err)
	}

	return &azureDataExplorerScaler{
		metadata:   azureDataExplorerMetadata,
		cache:      &sessionCache{metricValue: -1, metricThreshold: -1},
		name:       config.Name,
		namespace:  config.Namespace,
		httpClient: kedautil.CreateHTTPClient(config.GlobalHTTPTimeout),
	}, nil
}

func parseAzureDataExplorerMetadata(config *ScalerConfig) (*azureDataExplorerMetadata, error) {
	meta := azureDataExplorerMetadata{}
	switch config.PodIdentity {
	case "", kedav1alpha1.PodIdentityProviderNone:
		// Getting tenantId
		tenantID, err := getParameterFromConfig(config, "tenantId", true)
		if err != nil {
			return nil, err
		}
		meta.tenantID = tenantID

		// Getting clientId
		clientID, err := getParameterFromConfig(config, "clientId", true)
		if err != nil {
			return nil, err
		}
		meta.clientID = clientID

		// Getting clientSecret
		clientSecret, err := getParameterFromConfig(config, "clientSecret", true)
		if err != nil {
			return nil, err
		}
		meta.clientSecret = clientSecret

		meta.podIdentity = ""
	case kedav1alpha1.PodIdentityProviderAzure:
		meta.podIdentity = string(config.PodIdentity)
	default:
		return nil, fmt.Errorf("error parsing metadata. Details: Data Explorer Scaler doesn't support pod identity %s", config.PodIdentity)
	}

	// Getting clusterName
	clusterName, err := getParameterFromConfig(config, "clusterName", true)
	if err != nil {
		return nil, err
	}
	meta.clusterName = clusterName

	// Getting region - todo what is the region format
	region, err := getParameterFromConfig(config, "region", true)
	if err != nil {
		return nil, err
	}
	meta.region = region

	// Getting query, observe that we dont check AuthParams for query
	query, err := getParameterFromConfig(config, "query", false)
	if err != nil {
		return nil, err
	}
	meta.query = query

	// Getting threshold, observe that we dont check AuthParams for threshold
	val, err := getParameterFromConfig(config, "threshold", false)
	if err != nil {
		return nil, err
	}
	threshold, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing metadata. Details: can't parse threshold. Inner Error: %v", err)
	}
	meta.threshold = threshold

	// Resolve metricName
	if val, ok := config.TriggerMetadata["metricName"]; ok {
		meta.metricName = kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s", "azure-data-explorer", val, val)) //todo almog - what is val
	} else {
		meta.metricName = kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s", "azure-data-explorer", meta.clusterName, meta.region))
	}

	return &meta, nil
}

// IsActive determines if we need to scale from zero
func (s *azureDataExplorerScaler) IsActive(ctx context.Context) (bool, error) {
	err := s.updateCache()

	if err != nil {
		return false, fmt.Errorf("failed to execute IsActive function. Scaled object: %s. Namespace: %s. Inner Error: %v", s.name, s.namespace, err)
	}

	return s.cache.metricValue > 0, nil
}

func (s *azureDataExplorerScaler) GetMetricSpecForScaling() []v2beta2.MetricSpec {
	err := s.updateCache()

	if err != nil {
		DataExplorerLog.V(1).Info("failed to get metric spec.", "Scaled object", s.name, "Namespace", s.namespace, "Inner Error", err)
		return nil
	}

	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: s.metadata.metricName,
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: resource.NewQuantity(s.cache.metricThreshold, resource.DecimalSI),
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *azureDataExplorerScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	receivedMetric, err := s.getMetricData()

	if err != nil {
		return []external_metrics.ExternalMetricValue{}, fmt.Errorf("failed to get metrics. Scaled object: %s. Namespace: %s. Inner Error: %v", s.name, s.namespace, err)
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(receivedMetric.value, resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *azureDataExplorerScaler) Close() error {
	return nil
}

func (s *azureDataExplorerScaler) updateCache() error {
	if s.cache.metricValue < 0 {
		receivedMetric, err := s.getMetricData()

		if err != nil {
			return err
		}

		s.cache.metricValue = receivedMetric.value

		if receivedMetric.threshold > 0 {
			s.cache.metricThreshold = receivedMetric.threshold
		} else {
			s.cache.metricThreshold = s.metadata.threshold
		}
	}

	return nil
}

func (s *azureDataExplorerScaler) getMetricData() (metricsData, error) {
	tokenInfo, err := s.getAccessToken()
	if err != nil {
		return metricsData{}, err
	}

	metricsInfo, err := s.executeQuery(s.metadata.query, tokenInfo)
	if err != nil {
		return metricsData{}, err
	}

	DataExplorerLog.V(1).Info("Providing metric value", "metrics value", metricsInfo.value, "scaler name", s.name, "namespace", s.namespace)

	return metricsInfo, nil
}

func (s *azureDataExplorerScaler) getAccessToken() (tokenData, error) {
	// if there is no token yet or it will be expired in less, that 30 secs
	currentTimeSec := time.Now().Unix()
	tokenInfo := tokenData{}

	if s.metadata.podIdentity == "" {
		tokenInfo, _ = getTokenFromCache(s.metadata.clientID, s.metadata.clientSecret)
	} else {
		tokenInfo, _ = getTokenFromCache(s.metadata.podIdentity, s.metadata.podIdentity)
	}

	if currentTimeSec+30 > tokenInfo.ExpiresOn {
		newTokenInfo, err := s.refreshAccessToken()
		if err != nil {
			return tokenData{}, err
		}

		if s.metadata.podIdentity == "" {
			DataExplorerLog.V(1).Info("Token for Service Principal has been refreshed", "clientID", s.metadata.clientID, "scaler name", s.name, "namespace", s.namespace)
			_ = setTokenInCache(s.metadata.clientID, s.metadata.clientSecret, newTokenInfo)
		} else {
			DataExplorerLog.V(1).Info("Token for Pod Identity has been refreshed", "type", s.metadata.podIdentity, "scaler name", s.name, "namespace", s.namespace)
			_ = setTokenInCache(s.metadata.podIdentity, s.metadata.podIdentity, newTokenInfo)
		}

		return newTokenInfo, nil
	}
	return tokenInfo, nil
}

func (s *azureDataExplorerScaler) executeQuery(query string, tokenInfo tokenData) (metricsData, error) {
	queryData := queryResult{}
	var body []byte
	var statusCode int
	var err error

	body, statusCode, err = s.executeDataExplorerREST(query, tokenInfo)

	// Handle expired token
	if statusCode == 403 || (len(body) > 0 && strings.Contains(string(body), "TokenExpired")) {
		tokenInfo, err = s.refreshAccessToken()
		if err != nil {
			return metricsData{}, err
		}

		if s.metadata.podIdentity == "" {
			DataExplorerLog.V(1).Info("Token for Service Principal has been refreshed", "clientID", s.metadata.clientID, "scaler name", s.name, "namespace", s.namespace)
			_ = setTokenInCache(s.metadata.clientID, s.metadata.clientSecret, tokenInfo)
		} else {
			DataExplorerLog.V(1).Info("Token for Pod Identity has been refreshed", "type", s.metadata.podIdentity, "scaler name", s.name, "namespace", s.namespace)
			_ = setTokenInCache(s.metadata.podIdentity, s.metadata.podIdentity, tokenInfo)
		}

		if err == nil {
			body, statusCode, err = s.executeDataExplorerREST(query, tokenInfo)
		} else {
			return metricsData{}, err
		}
	}

	if statusCode != 200 && statusCode != 0 {
		return metricsData{}, fmt.Errorf("error processing Data Explorer request. HTTP code %d. Inner Error: %v. Body: %s", statusCode, err, string(body))
	}

	if err != nil {
		return metricsData{}, err
	}

	if len(body) == 0 {
		return metricsData{}, fmt.Errorf("error processing Data Explorer request. Details: empty body. HTTP code: %d", statusCode)
	}

	err = json.NewDecoder(bytes.NewReader(body)).Decode(&queryData)
	if err != nil {
		return metricsData{}, fmt.Errorf("error processing Data Explorer request. Details: can't decode response body to JSON from REST API result. HTTP code: %d. Inner Error: %v. Body: %s", statusCode, err, string(body))
	}

	if statusCode == 200 {
		metricsInfo := metricsData{}
		metricsInfo.threshold = s.metadata.threshold
		metricsInfo.value = 0

		// Pre-validation of query result:
		switch {
		case len(queryData.Tables) == 0 || len(queryData.Tables[0].Columns) == 0 || len(queryData.Tables[0].Rows) == 0:
			return metricsData{}, fmt.Errorf("error validating Data Explorer request. Details: there is no results after running your query. HTTP code: %d. Body: %s", statusCode, string(body))
		case len(queryData.Tables) > 1:
			return metricsData{}, fmt.Errorf("error validating Data Explorer request. Details: too many tables in query result: %d, expected: 1. HTTP code: %d. Body: %s", len(queryData.Tables), statusCode, string(body))
		case len(queryData.Tables[0].Rows) > 1:
			return metricsData{}, fmt.Errorf("error validating Data Explorer request. Details: too many rows in query result: %d, expected: 1. HTTP code: %d. Body: %s", len(queryData.Tables[0].Rows), statusCode, string(body))
		}

		if len(queryData.Tables[0].Rows[0]) > 0 {
			metricDataType := queryData.Tables[0].Columns[0].Type
			metricVal := queryData.Tables[0].Rows[0][0]
			parsedMetricVal, err := parseTableValueToInt64(metricVal, metricDataType)
			if err != nil {
				return metricsData{}, fmt.Errorf("%s. HTTP code: %d. Body: %s", err.Error(), statusCode, string(body))
			}
			metricsInfo.value = parsedMetricVal
		}

		if len(queryData.Tables[0].Rows[0]) > 1 {
			thresholdDataType := queryData.Tables[0].Columns[1].Type
			thresholdVal := queryData.Tables[0].Rows[0][1]
			parsedThresholdVal, err := parseTableValueToInt64(thresholdVal, thresholdDataType)
			if err != nil {
				return metricsData{}, fmt.Errorf("%s. HTTP code: %d. Body: %s", err.Error(), statusCode, string(body))
			}
			metricsInfo.threshold = parsedThresholdVal
		} else {
			metricsInfo.threshold = -1
		}

		return metricsInfo, nil
	}

	return metricsData{}, fmt.Errorf("error processing Data Explorer request. Details: unknown error. HTTP code: %d. Body: %s", statusCode, string(body))
}

func (s *azureDataExplorerScaler) refreshAccessToken() (tokenData, error) {
	tokenInfo, err := s.getAuthorizationToken()

	if err != nil {
		return tokenData{}, err
	}

	// Now, let's check we can use this token. If no, wait until we can use it
	currentTimeSec := time.Now().Unix()
	if currentTimeSec < tokenInfo.NotBefore {
		if currentTimeSec < tokenInfo.NotBefore+10 {
			sleepDurationSec := int(tokenInfo.NotBefore - currentTimeSec + 1)
			DataExplorerLog.V(1).Info("AAD token not ready", "delay (seconds)", sleepDurationSec, "scaler name", s.name, "namespace", s.namespace)
			time.Sleep(time.Duration(sleepDurationSec) * time.Second)
		} else {
			return tokenData{}, fmt.Errorf("error getting access token. Details: AAD token has been received, but start date begins in %d seconds, so current operation will be skipped", tokenInfo.NotBefore-currentTimeSec)
		}
	}

	return tokenInfo, nil
}

func (s *azureDataExplorerScaler) getAuthorizationToken() (tokenData, error) {
	var body []byte
	var statusCode int
	var err error
	var tokenInfo tokenData

	if s.metadata.podIdentity == "" {
		body, statusCode, err = s.executeAADApicall()
	} else {
		body, statusCode, err = s.executeIMDSApicall()
	}

	if err != nil {
		return tokenData{}, fmt.Errorf("error getting access token. HTTP code: %d. Inner Error: %v. Body: %s", statusCode, err, string(body))
	} else if len(body) == 0 {
		return tokenData{}, fmt.Errorf("error getting access token. Details: empty body. HTTP code: %d", statusCode)
	}

	err = json.NewDecoder(bytes.NewReader(body)).Decode(&tokenInfo)
	if err != nil {
		return tokenData{}, fmt.Errorf("error getting access token. Details: can't decode response body to JSON after getting access token. HTTP code: %d. Inner Error: %v. Body: %s", statusCode, err, string(body))
	}

	if statusCode == 200 {
		return tokenInfo, nil
	}

	return tokenData{}, fmt.Errorf("error getting access token. Details: unknown error. HTTP code: %d. Body: %s", statusCode, string(body))
}

func (s *azureDataExplorerScaler) executeDataExplorerREST(query string, tokenInfo tokenData) ([]byte, int, error) {
	m := map[string]interface{}{"query": query}

	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return nil, 0, fmt.Errorf("can't construct JSON for request to Data Explorer API. Inner Error: %v", err)
	}

	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf(deQueryEndpoint, s.metadata.clusterName, s.metadata.region), bytes.NewBuffer(jsonBytes)) // URL-encoded payload
	if err != nil {
		return nil, 0, fmt.Errorf("can't construct HTTP request to Data Explorer API. Inner Error: %v", err)
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", tokenInfo.AccessToken))
	request.Header.Add("Content-Length", fmt.Sprintf("%d", len(jsonBytes)))

	return s.runHTTP(request, "Data Explorer REST api")
}

func (s *azureDataExplorerScaler) executeAADApicall() ([]byte, int, error) {
	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {s.metadata.clientID},
		"redirect_uri":  {"http://"},
		"resource":      {"https://api.DataExplorer.io/"},
		"client_secret": {s.metadata.clientSecret},
	}

	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf(aadTokenEndpoint, s.metadata.tenantID), strings.NewReader(data.Encode())) // URL-encoded payload
	if err != nil {
		return nil, 0, fmt.Errorf("can't construct HTTP request to Azure Active Directory. Inner Error: %v", err)
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", fmt.Sprintf("%d", len(data.Encode())))

	return s.runHTTP(request, "AAD")
}

func (s *azureDataExplorerScaler) executeIMDSApicall() ([]byte, int, error) {
	request, err := http.NewRequest(http.MethodGet, fmt.Sprintf(deMiEndpoint, s.metadata.clusterName, s.metadata.region), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("can't construct HTTP request to Azure Instance Metadata service. Inner Error: %v", err)
	}

	request.Header.Add("Metadata", "true")

	return s.runHTTP(request, "IMDS")
}

func (s *azureDataExplorerScaler) runHTTP(request *http.Request, caller string) ([]byte, int, error) {
	request.Header.Add("Cache-Control", "no-cache")
	request.Header.Add("User-Agent", "keda/2.0.0")

	resp, err := s.httpClient.Do(request)
	if err != nil && resp != nil {
		return nil, resp.StatusCode, fmt.Errorf("error calling %s. Inner Error: %v", caller, err)
	} else if err != nil {
		return nil, 0, fmt.Errorf("error calling %s. Inner Error: %v", caller, err)
	}

	defer resp.Body.Close()
	s.httpClient.CloseIdleConnections()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("error reading %s response body: Inner Error: %v", caller, err)
	}

	return body, resp.StatusCode, nil
}
