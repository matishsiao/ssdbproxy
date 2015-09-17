package main

import (
	"sort"
)

type Configs struct {
	Debug		bool `json:"debug"`
	Host     string `json:"host"`
	Nodelist []struct {
		Host     string `json:"host"`
		ID       string `json:"id"`
		Password string `json:"password"`
		Port     int    `json:"port"`
		Weight   int    `json:"weight"`
	} `json:"nodelist"`
	Password string `json:"password"`
	Port     int    `json:"port"`
}

type sortedMap struct {
	m map[string]string
	s []string
}

func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] > sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]string) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key, _ := range m {
		sm.s[i] = key
		i++
	}
	sort.Strings(sm.s)
	return sm.s
}