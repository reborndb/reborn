// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import "encoding/json"

func args2Map(v interface{}) (m map[string]string) {
	d, _ := json.Marshal(v)
	json.Unmarshal(d, &m)
	return
}

func map2Args(v interface{}, m map[string]string) {
	d, _ := json.Marshal(m)
	json.Unmarshal(d, v)
}
