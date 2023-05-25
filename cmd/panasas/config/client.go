package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
)

const panasasHTTPS3MetadataHeader string = "Panasas-Config-Object-Metadata"

// ErrNotFound informs that the requested object has not been found by the
// config agent.
var ErrNotFound = fmt.Errorf("Not found")

// ErrNoRevisionYet informs that no successful modifying operation has been
// performed yet and consequently there is not cached config revision.
var ErrNoRevisionYet = fmt.Errorf("Config revision not retrieved yet")

// LockExpirationMilliseconds – time in milliseconds after which the Panasas config agent
// automatically releases a lock
const LockExpirationMilliseconds int = 3000

// ErrUnexpectedHTTPStatus informs about HTTP status value outside the expected
// range.
type ErrUnexpectedHTTPStatus uint

// ErrUnexpectedContentType informs about unexpected HTTP response content type
type ErrUnexpectedContentType string

func (e ErrUnexpectedHTTPStatus) Error() string {
	return fmt.Sprintf("Unexpected HTTP status: %v", uint(e))
}

func (e ErrUnexpectedContentType) Error() string {
	return fmt.Sprintf("Unexpected HTTP content type: %q", string(e))
}

// Client represents a Panasas config agent client
type Client struct {
	agentURL  string
	namespace string

	httpClient     *http.Client
	configRevision *string
}

// NewClient returns a configured Client
func NewClient(agentURL, namespace string) *Client {
	if agentURL == "" {
		return nil
	}
	client := Client{
		agentURL:   agentURL,
		namespace:  namespace,
		httpClient: &http.Client{},
	}
	return &client
}

func (c *Client) getConfigAgentURL(elem ...string) (*url.URL, error) {
	elems := make([]string, 0, 2+len(elem))
	elems = append(elems, "namespaces", c.namespace)
	elems = append(elems, elem...)

	urlPath := path.Join(elems...)

	slash := "/"
	separator := slash
	offset := 0
	for strings.HasPrefix(urlPath[offset:], slash) {
		offset++
	}
	if strings.HasSuffix(c.agentURL, slash) {
		separator = ""
	}

	return url.Parse(strings.Join([]string{c.agentURL, urlPath[offset:]}, separator))
}

func (c *Client) makeConfigAgentRequest(urlElem ...string) (*http.Request, error) {
	u, err := c.getConfigAgentURL(urlElem...)
	if err != nil {
		log.Printf("Failed formatting URL for %v: %s\n", urlElem, err)
		return nil, err
	}

	req := http.Request{
		Method:        http.MethodGet,
		URL:           u,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Host:          u.Host,
		ContentLength: 0,
	}
	return &req, nil
}

// closeResponseBody close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
// (copied from minio-go)
func closeResponseBody(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

func (c *Client) listConfigObjects(prefix, delimiter string) ([]string, error) {
	req, err := c.makeConfigAgentRequest("configs")
	if err != nil {
		log.Printf("Failed preparing HTTP request object for /objects with prefix %q: %s\n", prefix, err)
		return []string{}, err
	}

	q := req.URL.Query()
	q.Add("prefix", prefix)
	if delimiter != "" {
		q.Add("delimiter", delimiter)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("HTTP request failed: %s\n", err)
		return []string{}, err
	}
	defer closeResponseBody(resp)
	expectedContentType := "application/json"
	contentType := resp.Header.Get("content-type")
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = contentType[:idx]
	}
	if contentType != expectedContentType {
		return []string{}, ErrUnexpectedContentType(contentType)
	}

	dec := json.NewDecoder(resp.Body)
	var result []string
	err = dec.Decode(&result)
	if err != nil {
		log.Printf("JSON decoding failed: %s\n", err)
		return []string{}, err
	}

	return result, nil
}

// GetObjectsList returns a list of objects with names beginning with the
// specified prefix.
func (c *Client) GetObjectsList(prefix string) ([]string, error) {
	return c.listConfigObjects(prefix, "")
}

// GetObjectPrefixes returns a list of shared object name prefixes.
//
// GetObjectPrefixes will group the objects with names matching the specified
// prefix by trimming the parts beginning after the first occurrence of the
// delimiter after the prefix.
// E.g. let's assume 5 objects are stored with the following keys:
// - "/home/user1/object1"
// - "/home/user1/object2"
// - "/home/user2/object1"
// - "/home/user2/object2"
// - "/etc/share_object"
// In this case GetObjectPrefixes("/home/", "/") will return the following list
// of common prefixes:
// - "/home/user1",
// - "/home/user2".
//
// This will be done by performing the following algorithm:
// /home/                 - prefix
// /home/user1/object1    - object name
// ^^^^^^                 - matches the prefix
//
//            ^           - is the first delimiter AFTER the prefix
//
//            ^^^^^^^^    - delimiter with the following part is trimmed
func (c *Client) GetObjectPrefixes(prefix, delimiter string) ([]string, error) {
	return c.listConfigObjects(prefix, delimiter)
}

// GetObject returns an object:
// - dataReader is the reader returning the data of the object,
// - metadata is the metadata of the object.
// The caller is responsible for calling Close() on the dataReader.
// If err is not nil, dataReader and metadata are assumed invalid and should
// not be used.
func (c *Client) GetObject(objectName string) (dataReader io.ReadCloser, oi *ObjectInfo, err error) {
	base := "configs"

	req, err := c.makeConfigAgentRequest(base, objectName)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, objectName, err)
		return nil, nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP request failed with error %w", err)
	}

	defer func() {
		if err != nil {
			closeResponseBody(resp)
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil, ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	expectedContentType := "application/octet-stream"
	contentType := resp.Header.Get("content-type")
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = contentType[:idx]
	}
	if contentType != expectedContentType {
		err = ErrUnexpectedContentType(contentType)
		return nil, nil, err
	}

	metadata := resp.Header.Get(panasasHTTPS3MetadataHeader)
	oi, err = parseObjectInfo(metadata)
	if err != nil {
		log.Printf(
			"Failed parsing object info for %v from metadata %q: %s\n",
			objectName,
			metadata,
			err,
		)
		return nil, nil, err
	}
	oi.ByteLength = resp.ContentLength

	return resp.Body, oi, nil
}

func (c *Client) deleteObjects(objectName string, byPrefix bool, lockID ...string) error {
	base := "configs"

	req, err := c.makeConfigAgentRequest(base, objectName)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, objectName, err)
		return err
	}

	req.Method = http.MethodDelete

	q := req.URL.Query()
	hasQuery := false
	if len(lockID) != 0 && lockID[0] != "" {
		q.Add("lock_id", lockID[0])
		hasQuery = true
	}
	if byPrefix {
		q.Add("by_prefix", "1")
		hasQuery = true
	}
	if hasQuery {
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	var ni NamespaceInfo
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&ni)
	if err != nil {
		return fmt.Errorf("Cannot decode namespace info - JSON decoding error: %w", err)
	}

	revision := ni.Revision
	c.configRevision = &revision
	return nil
}

// DeleteObject deletes an object with matching name
func (c *Client) DeleteObject(objectName string, lockID ...string) error {
	return c.deleteObjects(objectName, false, lockID...)
}

// DeleteObjectsByPrefix deletes all objects with names starting with the
// specified prefix.
func (c *Client) DeleteObjectsByPrefix(prefix string) error {
	return c.deleteObjects(prefix, true)
}

// PutObject stores an object of a given name in the config agent
func (c *Client) PutObject(objectName string, data io.Reader, lockID ...string) error {
	base := "configs"

	req, err := c.makeConfigAgentRequest(base, objectName)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, objectName, err)
		return err
	}

	if data == nil {
		data = bytes.NewBuffer([]byte{})
	}
	body := io.NopCloser(data)

	req.Method = http.MethodPut
	req.Body = body
	req.Header.Set("Content-Type", "application/octet-stream")

	if len(lockID) != 0 && lockID[0] != "" {
		q := req.URL.Query()
		q.Add("lock_id", lockID[0])

		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("HTTP request failed with error %w", err)
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		var oi ObjectInfo
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&oi)
		if err != nil {
			return fmt.Errorf("Cannot decode namespace info - JSON decoding error: %w", err)
		}

		revision := oi.Namespace.Revision
		c.configRevision = &revision
		return nil
	}

	return ErrUnexpectedHTTPStatus(resp.StatusCode)
}

// GetObjectInfo fetches object metadata from the config agent
func (c *Client) GetObjectInfo(objectName string) (oi *ObjectInfo, err error) {
	base := "configs"

	req, err := c.makeConfigAgentRequest(base, objectName)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, objectName, err)
		return nil, err
	}

	req.Method = http.MethodHead
	resp, err := c.httpClient.Do(req)
	closeResponseBody(resp)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	metadata := resp.Header.Get(panasasHTTPS3MetadataHeader)
	oi, err = parseObjectInfo(metadata)
	if err != nil {
		log.Printf(
			"Failed parsing object info for %v from metadata %q: %s\n",
			objectName,
			metadata,
			err,
		)
		return nil, err
	}
	oi.ByteLength = resp.ContentLength

	return oi, nil
}

// GetRecentConfigRevision returns the config revision reported by the most
// recent config modifying operation (PutObject/DeleteObject)
func (c *Client) GetRecentConfigRevision() (revision string, err error) {
	if c.configRevision == nil {
		return "", ErrNoRevisionYet
	}
	return *c.configRevision, nil
}

func (c *Client) fetchNamespaceInfo(endpoint, httpMethod string) (*NamespaceInfo, error) {
	req, err := c.makeConfigAgentRequest(endpoint)
	if err != nil {
		if endpoint != "" {
			log.Printf("Failed preparing HTTP request object for /namespaces/%s/%s: %s\n", c.namespace, endpoint, err)
		} else {
			log.Printf("Failed preparing HTTP request object for /namespaces/%s: %s\n", c.namespace, err)
		}
		return nil, err
	}
	req.Method = httpMethod

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed with error %w", err)
	}

	if resp.StatusCode == http.StatusOK {
		infoData, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return parseNamespaceInfo(string(infoData))
	}

	return nil, ErrUnexpectedHTTPStatus(resp.StatusCode)
}

// GetConfigRevision queries the config agent for the current config revision
func (c *Client) GetConfigRevision() (revision string, err error) {
	endpoint := ""
	method := http.MethodGet

	info, err := c.fetchNamespaceInfo(endpoint, method)
	if err != nil {
		return "", err
	}
	rev := info.Revision
	c.configRevision = &rev

	return rev, nil
}

// UpdateConfigRevision forces Panasas config agent to generate a new revision
// string
func (c *Client) UpdateConfigRevision() (info *NamespaceInfo, err error) {
	endpoint := "actions/Revision.Update"
	method := http.MethodPost

	return c.fetchNamespaceInfo(endpoint, method)
}

// ClearCache triggers Panasas config agent cache purging
func (c *Client) ClearCache() (info *NamespaceInfo, err error) {
	endpoint := "actions/Cache.Clear"
	method := http.MethodPost

	return c.fetchNamespaceInfo(endpoint, method)
}

// GetObjectLock tries to get a write or read lock on the specified object
// Set "read" to true to obtain a non-exclusive read lock.
// Returns the ID of the obtained lock. This ID can be then used in the calls
// to ReleaseObjectLock(), PutObject(), DeleteObject().
func (c *Client) GetObjectLock(objectName string, read bool) (lockID string, err error) {
	base := "/lock"

	req, err := c.makeConfigAgentRequest(base, objectName)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, objectName, err)
		return "", err
	}

	req.Method = http.MethodPost

	if read != false {
		q := req.URL.Query()
		q.Add("type", "read")

		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	expectedContentType := "text/plain"
	contentType := resp.Header.Get("content-type")
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = contentType[:idx]
	}
	if contentType != expectedContentType {
		err = ErrUnexpectedContentType(contentType)
		return
	}

	lockIDBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	lockID = string(lockIDBytes)
	return lockID, nil
}

// ReleaseObjectLock releases a previously obtained object lock
// Will return ErrNotFound if the specified lock has not been found or the lock
// has expired since it was obtained.
func (c *Client) ReleaseObjectLock(lockID string) (err error) {
	base := "/locks"
	req, err := c.makeConfigAgentRequest(base, lockID)
	if err != nil {
		log.Printf("Failed preparing HTTP request object for %v with name %q: %s\n", base, lockID, err)
		return err
	}

	req.Method = http.MethodDelete

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("HTTP request failed with error %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	return nil
}

func (c *Client) String() string {
	return fmt.Sprintf("Client(URL: %q, NS: %q)", c.agentURL, c.namespace)
}
