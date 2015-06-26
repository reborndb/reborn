// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/juju/errors"
)

//call http url and get json, then decode to objptr
func httpCall(objPtr interface{}, url string, method string, arg interface{}) error {
	rw := &bytes.Buffer{}
	if arg != nil {
		buf, err := json.Marshal(arg)
		if err != nil {
			return errors.Trace(err)
		}
		rw.Write(buf)
	}

	req, err := http.NewRequest(method, url, rw)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}

	if resp.StatusCode/100 != 2 {
		return errors.Errorf("error: %d, message: %s", resp.StatusCode, string(body))
	}

	if objPtr != nil {
		return json.Unmarshal(body, objPtr)
	}

	return nil
}
