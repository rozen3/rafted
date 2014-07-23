package comm

import (
    "bytes"
    "errors"
    "net/http"
)

type HTTPTransport struct {
    url    string
    client *http.Client
    resp   *http.Response
}

func NewHTTPTransport(url string) *HTTPTransport {
    return &HTTPTransport{
        url:    url,
        client: &http.Client{},
    }
}

func (self *HTTPTransport) Open() error {
    // empty body
    return nil
}

func (self *HTTPTransport) Close() error {
    if self.resp != nil {
        self.resp.Body.Close()
    }
    return nil
}

func (self *HTTPTransport) Read(b []byte) (int, error) {
    if self.resp == nil {
        return 0, errors.New("nothing to read")
    }
    return self.resp.Body.Read(b)
}

func (self *HTTPTransport) Write(b []byte) (int, error) {
    if self.resp != nil {
        self.resp.Body.Close()
    }
    body := bytes.NewReader(b)
    req, err := http.NewRequest("GET", self.url, body)
    if err != nil {
        return 0, err
    }

    resp, err := self.client.Do(req)
    if err != nil {
        return len(b), err
    }
    self.resp = resp
    return len(b), nil
}
