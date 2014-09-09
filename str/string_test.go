package str

import (
    "github.com/hhkbp2/testify/assert"
    "regexp"
    "testing"
)

func TestRandomString(t *testing.T) {
    size := uint32(1000)
    s1 := RandomString(size)
    s2 := RandomString(size)
    assert.NotEqual(t, s1, s2)
}

func TestRandomIP(t *testing.T) {
    ip := RandomIP()
    r, err := regexp.Compile(`\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}`)
    assert.Nil(t, err)
    assert.True(t, r.MatchString(ip), ip)
    ip2 := RandomIP()
    assert.NotEqual(t, ip, ip2)
}
