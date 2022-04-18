package service

import "testing"

func TestIsAttributes(t *testing.T) {
    js := `{"properties":{"attributes":{"hello":"world"}}}`
    t.Log(isAttributes(js))
}
